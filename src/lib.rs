/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Standardised data storage for optical spectroscopy.
//!
//! Wray stores wavelengths, spatial measurements, and intensity spectra in a
//! compact binary file wrapping Apache Arrow IPC streams.
//!
//! # Lifecycle
//!
//! 1. **Create** a [`Dataset`] with [`Dataset::new`].
//! 2. **Push** wavelengths, measurements, and intensities via table accessors.
//! 3. **Close** with [`Dataset::close`] (appendable) or [`Dataset::finish`]
//!    (sealed, Arrow file format). Use [`Dataset::finish_to`] to write a
//!    finished copy without consuming the original.
//! 4. **Reopen** with [`Dataset::new`] or [`Dataset::open`] — unfinished files
//!    are automatically opened for appending.
//! 5. **Read** with [`Dataset::open`] and the `read_*` methods.
//!
//! # File format
//!
//! ```text
//! [Header 24 B] [Segment …] [Segment …] … [Manifest TOML]
//! ```
//!
//! The 24-byte header stores magic bytes (`WRAY`), format version, a finished
//! flag, and the manifest offset/length. Each segment holds Arrow IPC stream
//! data. The TOML manifest at the end indexes all segments and stores
//! experiment metadata. On [`finish`](Dataset::finish), segments are
//! consolidated into Arrow IPC **file** format for random-access reads.

/* ----------------------------------------------------------------------------- Private Modules */

mod dataset;
mod error;
mod format;
mod intensities;
mod measurements;
mod util;
mod wavelengths;
mod writer;

/* ------------------------------------------------------------------------------ Public Exports */

pub use self::dataset::Dataset;
pub use self::error::Error;
pub use self::format::{Config, Format, Manifest, Units};

/// A single wavelength entry returned by read queries.
pub type Wavelength = wavelengths::record::Record;

/// A single measurement entry returned by read queries.
pub type Measurement = measurements::record::Record;

/// A single intensity entry returned by read queries.
pub type Intensity = intensities::record::Record;

/* ---------------------------------------------------------------------------------- Unit Tests */

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::NamedTempFile;

    use super::*;

    const XY: Config = Config {
        x: Some(Units::Length),
        y: Some(Units::Length),
        z: None,
        a: None,
        b: None,
        c: None,
    };

    const XYZA: Config = Config {
        x: Some(Units::Length),
        y: Some(Units::Length),
        z: Some(Units::Length),
        a: Some(Units::Angle),
        b: None,
        c: None,
    };

    fn tmp() -> PathBuf {
        NamedTempFile::new()
            .expect("create temp file")
            .into_temp_path()
            .to_path_buf()
    }

    /* ------------------------------------------------------------------------- Round-trip test */

    #[test]
    fn round_trip() {
        let path = tmp();
        let wavelengths_nm = vec![400.0, 500.0, 600.0, 700.0];
        let n = wavelengths_nm.len();

        // Write
        {
            let mut ds = Dataset::new(&path, &XY).expect("create dataset");
            let wl_ids = ds.wavelengths().push(&wavelengths_nm).expect("push wl");
            assert_eq!(wl_ids, vec![0, 1, 2, 3]);

            let id = ds
                .measurements()
                .push(Some(0.001), Some(0.002), None, None, None, None, 100_000)
                .expect("push");
            assert_eq!(id, 0);

            ds.intensities()
                .push(id, &wl_ids, &[0.1, 0.2, 0.3, 0.4])
                .expect("push intensities");

            ds.close().expect("close");
        }

        // Read
        {
            let ds = Dataset::open(&path).expect("open dataset");
            assert!(!ds.is_finished());

            let wl = ds.read_wavelengths().expect("read wavelengths");
            assert_eq!(wl.len(), n);
            assert_eq!(wl[0].id, 0);
            assert!((wl[0].nm - 400.0).abs() < 1e-6);
            assert_eq!(wl[3].id, 3);

            let ms = ds.read_measurements().expect("read measurements");
            assert_eq!(ms.len(), 1);
            assert_eq!(ms[0].id, 0);
            assert!(ms[0].x.is_some());
            assert!(ms[0].z.is_none());

            let it = ds.read_intensities().expect("read intensities");
            assert_eq!(it.len(), n);
            assert_eq!(it[0].measurement, 0);
            assert_eq!(it[0].wavelength, 0);
            assert!((it[0].intensity - 0.1).abs() < 1e-12);
        }
    }

    /* ----------------------------------------------------------------------------- Drop safety */

    #[test]
    fn drop_writes_file() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            ds.wavelengths().push(&[500.0]).expect("push wl");
            ds.measurements()
                .push(None, None, None, None, None, None, 50_000)
                .expect("push");
        }
        let ds = Dataset::open(&path).expect("open after drop");
        assert_eq!(ds.read_wavelengths().expect("read").len(), 1);
        assert_eq!(ds.read_measurements().expect("read").len(), 1);
    }

    /* -------------------------------------------------------------------- Optional coordinates */

    #[test]
    fn optional_coordinates() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            ds.measurements()
                .push(None, None, None, None, None, None, 10_000)
                .expect("push none");
            ds.measurements()
                .push(Some(0.005), None, None, None, None, None, 10_000)
                .expect("push x");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        let ms = ds.read_measurements().expect("read");
        assert_eq!(ms.len(), 2);
        assert!(ms[0].x.is_none());
        assert!(ms[0].y.is_none());
        assert!(ms[1].x.is_some());
        assert!((ms[1].x.expect("x present") - 0.005).abs() < 1e-6);
    }

    /* ----------------------------------------------------------------------------- Calibration */

    #[test]
    fn calibration_marker() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            let id = ds
                .measurements()
                .push(None, None, None, None, None, None, 10_000)
                .expect("push");
            ds.calibration(id);
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        assert_eq!(ds.manifest().calibrations, vec![0]);
    }

    /* ----------------------------------------------------------------------------- Finish modes */

    #[test]
    fn finish_sets_flag() {
        let path = tmp();
        {
            let ds = Dataset::new(&path, &XY).expect("create");
            ds.finish().expect("finish");
        }
        let ds = Dataset::open(&path).expect("open");
        assert!(ds.is_finished());
    }

    #[test]
    fn finish_converts_to_file_format() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            let ids = ds.wavelengths().push(&[400.0, 500.0]).expect("push wl");
            let m = ds
                .measurements()
                .push(Some(0.001), None, None, None, None, None, 50_000)
                .expect("push");
            ds.intensities().push(m, &ids, &[1.0, 2.0]).expect("push it");
            ds.finish().expect("finish");
        }
        let ds = Dataset::open(&path).expect("open");
        assert!(ds.is_finished());
        assert_eq!(ds.read_wavelengths().expect("wl").len(), 2);
        assert_eq!(ds.read_measurements().expect("ms").len(), 1);
        assert_eq!(ds.read_intensities().expect("it").len(), 2);
    }

    #[test]
    fn finish_to_creates_separate_file() {
        let path = tmp();
        let finished_path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            ds.wavelengths().push(&[400.0]).expect("push wl");
            ds.measurements()
                .push(None, None, None, None, None, None, 10_000)
                .expect("push");
            ds.finish_to(&finished_path).expect("finish_to");
            // Original should still be usable (not closed).
            ds.wavelengths().push(&[500.0]).expect("push more");
            ds.close().expect("close");
        }
        // Finished copy.
        let finished = Dataset::open(&finished_path).expect("open finished");
        assert!(finished.is_finished());
        assert_eq!(finished.read_wavelengths().expect("wl").len(), 1);

        // Original — not finished, has extra data.
        let original = Dataset::open(&path).expect("open original");
        assert!(!original.is_finished());
        assert_eq!(original.read_wavelengths().expect("wl").len(), 2);
    }

    #[test]
    fn finished_is_read_only() {
        let path = tmp();
        {
            let ds = Dataset::new(&path, &XY).expect("create");
            ds.finish().expect("finish");
        }
        let ds = Dataset::open(&path).expect("open");
        assert!(ds.is_finished());
    }

    /* -------------------------------------------------------------------------- Units manifest */

    #[test]
    fn units_in_manifest() {
        let path = tmp();
        {
            let ds = Dataset::new(&path, &XYZA).expect("create");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        assert_eq!(ds.manifest().axes.x, Some(Units::Length));
        assert_eq!(ds.manifest().axes.y, Some(Units::Length));
        assert_eq!(ds.manifest().axes.z, Some(Units::Length));
        assert_eq!(ds.manifest().axes.a, Some(Units::Angle));
    }

    /* ---------------------------------------------------------------- Wavelength deduplication */

    #[test]
    fn wavelength_dedup() {
        let path = tmp();
        let mut ds = Dataset::new(&path, &XY).expect("create");
        let ids1 = ds.wavelengths().push(&[400.0, 500.0]).expect("first push");
        let ids2 = ds.wavelengths().push(&[400.0, 600.0]).expect("second push");
        assert_eq!(ids1, vec![0, 1]);
        assert_eq!(ids2, vec![0, 2]);
    }

    /* ------------------------------------------- Full coordinates (x, y, z, a) */

    #[test]
    fn full_coordinates() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XYZA).expect("create");
            let x = 0.001;
            let y = 0.002;
            let z = 3e-6;
            let a = std::f32::consts::FRAC_PI_4;
            ds.measurements()
                .push(Some(x), Some(y), Some(z), Some(a), None, None, 100_000)
                .expect("push");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        let ms = ds.read_measurements().expect("read");
        assert_eq!(ms.len(), 1);
        assert!((ms[0].x.expect("x") - 0.001).abs() < 1e-6);
        assert!((ms[0].y.expect("y") - 0.002).abs() < 1e-6);
        assert!((ms[0].z.expect("z") - 3e-6).abs() < 1e-9);
        assert!((ms[0].a.expect("a") - std::f32::consts::FRAC_PI_4).abs() < 1e-6);
    }

    /* ------------------------------------------- Relative timestamps */

    #[test]
    fn timestamps_are_relative() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            ds.measurements()
                .push(None, None, None, None, None, None, 10_000)
                .expect("push");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        let ms = ds.read_measurements().expect("read");
        assert!(ms[0].timestamp < 1_000_000);
    }

    /* ------------------------------------------------------------------- Reopen and append */

    #[test]
    fn open_appends_data() {
        let path = tmp();
        let wl_nm = [400.0, 500.0, 600.0];

        // Write initial data and close.
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            let ids = ds.wavelengths().push(&wl_nm).expect("push wl");
            let m = ds
                .measurements()
                .push(Some(0.001), Some(0.002), None, None, None, None, 50_000)
                .expect("push");
            ds.intensities().push(m, &ids, &[1.0, 2.0, 3.0]).expect("push it");
            ds.close().expect("close");
        }

        // Open (unfinished) and append more data.
        {
            let mut ds = Dataset::open(&path).expect("open");
            let ids = ds.wavelengths().push(&[400.0, 700.0]).expect("push wl2");
            assert_eq!(ids[0], 0); // deduped
            assert_eq!(ids[1], 3); // new

            let m = ds
                .measurements()
                .push(Some(0.003), None, None, None, None, None, 60_000)
                .expect("push2");
            assert_eq!(m, 1);
            ds.intensities().push(m, &ids, &[4.0, 5.0]).expect("push it2");
            ds.close().expect("close2");
        }

        // Verify all data.
        let ds = Dataset::open(&path).expect("open");
        assert_eq!(ds.read_wavelengths().expect("wl").len(), 4);
        assert_eq!(ds.read_measurements().expect("ms").len(), 2);
        assert_eq!(ds.read_intensities().expect("it").len(), 5);
    }

    #[test]
    fn multiple_open_cycles() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            ds.wavelengths().push(&[400.0]).expect("wl");
            ds.measurements()
                .push(None, None, None, None, None, None, 10_000)
                .expect("push");
            ds.close().expect("close");
        }
        for cycle in 1..=3 {
            let mut ds = Dataset::open(&path).expect("open");
            ds.measurements()
                .push(None, None, None, None, None, None, 10_000)
                .expect("push");
            ds.close().expect("close");
            let ds = Dataset::open(&path).expect("open");
            assert_eq!(ds.read_measurements().expect("ms").len(), cycle + 1);
        }
    }

    /* ---------------------------------------------------------------- Query API guards */

    #[test]
    fn is_finished_flag() {
        let path = tmp();
        {
            let ds = Dataset::new(&path, &XY).expect("create");
            ds.close().expect("close");
        }
        let unfinished = Dataset::open(&path).expect("open");
        assert!(!unfinished.is_finished());

        let path2 = tmp();
        {
            let ds = Dataset::new(&path2, &XY).expect("create");
            ds.finish().expect("finish");
        }
        let finished = Dataset::open(&path2).expect("open");
        assert!(finished.is_finished());
    }

    /* ----------------------------------------------------------------- Niche optimization */

    #[test]
    fn units_niche_optimised() {
        assert_eq!(size_of::<Option<Units>>(), size_of::<Units>());
    }

    /* ------------------------------------------------------------------- New dispatch */

    #[test]
    fn new_opens_existing() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            ds.wavelengths().push(&[400.0]).expect("push wl");
            ds.close().expect("close");
        }
        // new() on existing file should open it for appending.
        {
            let mut ds = Dataset::new(&path, &XY).expect("open via new");
            ds.wavelengths().push(&[500.0]).expect("push more");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        assert_eq!(ds.read_wavelengths().expect("wl").len(), 2);
    }
}
