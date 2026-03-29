/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Standardised data storage for optical spectroscopy.
//!
//! The `wray` format stores wavelengths, spatial measurements, and intensity spectra in a compact
//! binary file wrapping Apache Arrow IPC streams.
//!
//! # Lifecycle
//!
//! 1. **Create** a new file with [`new`][1].
//! 2. **Push** wavelengths, measurements, and intensities.
//! 3. [`Close`][2] the file once all data has been written.
//! 4. [`Open`][3] existing files to add additional data if required.
//! 5. Convert to read-only for improved random access read performance and reduced file size. Use
//!    [`finish`][4] to consume the existing file and replace in situ. Use [`snapshot`][5] to copy
//!    the data while leaving the original file intact e.g. for interim analysis.
//!
//! # File format
//!
//! ```text
//! [Header 23 B] [Segment …] [Segment …] … [Manifest TOML]
//! ```
//!
//! The 23-byte header stores magic bytes `WRAY`, format version, finished flag, and the manifest
//! offset. Each segment holds Arrow IPC stream data. The manifest TOML at the end indexes all
//! segments and stores experiment metadata.
//!
//! [1]: unfinished::Dataset::new
//! [2]: unfinished::Dataset::close
//! [3]: Dataset::open
//! [4]: unfinished::Dataset::finish
//! [5]: unfinished::Dataset::snapshot

/* ----------------------------------------------------------------------------- Private Modules */

mod dataset;
mod error;
mod format;
mod intensities;
mod measurements;
mod util;
mod wavelengths;
mod table;

/* ------------------------------------------------------------------------------ Public Exports */

pub use self::dataset::Dataset;
pub use self::dataset::finished;
pub use self::dataset::unfinished;
pub use self::error::Error;
pub use self::format::{Config, Format, Manifest, Units};

/// A single wavelength record from the dataset.
pub type Wavelength = wavelengths::record::Record;

/// A single measurement record from the dataset.
pub type Measurement = measurements::record::Record;

/// A single intensity record from the dataset.
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
            .expect("unable to create temporary file")
            .into_temp_path()
            .to_path_buf()
    }

    fn expect_unfinished(path: &std::path::Path) -> unfinished::Dataset {
        let Dataset::Unfinished(ds) = Dataset::open(path).expect("open") else {
            panic!("expected unfinished dataset")
        };
        ds
    }

    fn expect_finished(path: &std::path::Path) -> finished::Dataset {
        let Dataset::Finished(ds) = Dataset::open(path).expect("open") else {
            panic!("expected finished dataset")
        };
        ds
    }

    /* ------------------------------------------------------------------------- Round-trip test */

    #[test]
    fn round_trip() {
        let path = tmp();
        let wavelengths_nm = vec![400.0, 500.0, 600.0, 700.0];
        let n = wavelengths_nm.len();

        // Write
        {
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create dataset");
            let wl_ids = ds.wavelengths.push(&wavelengths_nm).expect("push wl");
            assert_eq!(wl_ids, vec![0, 1, 2, 3]);

            let id = ds
                .measurements
                .push(Some(0.001), Some(0.002), None, None, None, None, 100_000)
                .expect("push");
            assert_eq!(id, 0);

            ds.intensities
                .push(id, &wl_ids, &[0.1, 0.2, 0.3, 0.4])
                .expect("push intensities");

            ds.close().expect("close");
        }

        // Read
        {
            let ds = expect_unfinished(&path);
            let wl = ds.wavelengths.read().expect("read wavelengths");
            assert_eq!(wl.len(), n);
            assert_eq!(wl[0].id, 0);
            assert!((wl[0].nm - 400.0).abs() < 1e-6);
            assert_eq!(wl[3].id, 3);

            let ms = ds.measurements.read().expect("read measurements");
            assert_eq!(ms.len(), 1);
            assert_eq!(ms[0].id, 0);
            assert!(ms[0].x.is_some());
            assert!(ms[0].z.is_none());

            let it = ds.intensities.read().expect("read intensities");
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
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
            ds.wavelengths.push(&[500.0]).expect("push wl");
            ds.measurements
                .push(None, None, None, None, None, None, 50_000)
                .expect("push");
        }
        let ds = expect_unfinished(&path);
        assert_eq!(ds.wavelengths.read().expect("read").len(), 1);
        assert_eq!(ds.measurements.read().expect("read").len(), 1);
    }

    /* -------------------------------------------------------------------- Optional coordinates */

    #[test]
    fn optional_coordinates() {
        let path = tmp();
        {
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
            ds.measurements
                .push(None, None, None, None, None, None, 10_000)
                .expect("push none");
            ds.measurements
                .push(Some(0.005), None, None, None, None, None, 10_000)
                .expect("push x");
            ds.close().expect("close");
        }
        let ds = expect_unfinished(&path);
        let ms = ds.measurements.read().expect("read");
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
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
            let id = ds
                .measurements
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
            let ds = unfinished::Dataset::new(&path, &XY).expect("create");
            ds.finish().expect("finish");
        }
        let ds = Dataset::open(&path).expect("open");
        assert!(ds.is_finished());
    }

    #[test]
    fn finish_converts_to_file_format() {
        let path = tmp();
        {
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
            let ids = ds.wavelengths.push(&[400.0, 500.0]).expect("push wl");
            let m = ds
                .measurements
                .push(Some(0.001), None, None, None, None, None, 50_000)
                .expect("push");
            ds.intensities.push(m, &ids, &[1.0, 2.0]).expect("push it");
            ds.finish().expect("finish");
        }
        let ds = expect_finished(&path);
        assert_eq!(ds.wavelengths.read().expect("wl").len(), 2);
        assert_eq!(ds.measurements.read().expect("ms").len(), 1);
        assert_eq!(ds.intensities.read().expect("it").len(), 2);
    }

    #[test]
    fn snapshot_creates_separate_file() {
        let path = tmp();
        let finished_path = tmp();
        {
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
            ds.wavelengths.push(&[400.0]).expect("push wl");
            ds.measurements
                .push(None, None, None, None, None, None, 10_000)
                .expect("push");
            ds.snapshot(&finished_path).expect("snapshot");
            // Original should still be usable (not closed).
            ds.wavelengths.push(&[500.0]).expect("push more");
            ds.close().expect("close");
        }
        // Finished copy.
        let snap = expect_finished(&finished_path);
        assert_eq!(snap.wavelengths.read().expect("wl").len(), 1);

        // Original — not finished, has extra data.
        let original = expect_unfinished(&path);
        assert_eq!(original.wavelengths.read().expect("wl").len(), 2);
    }

    #[test]
    fn finished_is_read_only() {
        let path = tmp();
        {
            let ds = unfinished::Dataset::new(&path, &XY).expect("create");
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
            let ds = unfinished::Dataset::new(&path, &XYZA).expect("create");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        assert_eq!(ds.manifest().cfg.x, Some(Units::Length));
        assert_eq!(ds.manifest().cfg.y, Some(Units::Length));
        assert_eq!(ds.manifest().cfg.z, Some(Units::Length));
        assert_eq!(ds.manifest().cfg.a, Some(Units::Angle));
    }

    /* ---------------------------------------------------------------- Wavelength deduplication */

    #[test]
    fn wavelength_dedup() {
        let path = tmp();
        let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
        let ids1 = ds.wavelengths.push(&[400.0, 500.0]).expect("first push");
        let ids2 = ds.wavelengths.push(&[400.0, 600.0]).expect("second push");
        assert_eq!(ids1, vec![0, 1]);
        assert_eq!(ids2, vec![0, 2]);
    }

    /* ------------------------------------------- Full coordinates (x, y, z, a) */

    #[test]
    fn full_coordinates() {
        let path = tmp();
        {
            let mut ds = unfinished::Dataset::new(&path, &XYZA).expect("create");
            let x = 0.001;
            let y = 0.002;
            let z = 3e-6;
            let a = std::f32::consts::FRAC_PI_4;
            ds.measurements
                .push(Some(x), Some(y), Some(z), Some(a), None, None, 100_000)
                .expect("push");
            ds.close().expect("close");
        }
        let ds = expect_unfinished(&path);
        let ms = ds.measurements.read().expect("read");
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
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
            ds.measurements
                .push(None, None, None, None, None, None, 10_000)
                .expect("push");
            ds.close().expect("close");
        }
        let ds = expect_unfinished(&path);
        let ms = ds.measurements.read().expect("read");
        assert!(ms[0].timestamp < 1_000_000);
    }

    /* ------------------------------------------------------------------- Reopen and append */

    #[test]
    fn open_appends_data() {
        let path = tmp();
        let wl_nm = [400.0, 500.0, 600.0];

        // Write initial data and close.
        {
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
            let ids = ds.wavelengths.push(&wl_nm).expect("push wl");
            let m = ds
                .measurements
                .push(Some(0.001), Some(0.002), None, None, None, None, 50_000)
                .expect("push");
            ds.intensities
                .push(m, &ids, &[1.0, 2.0, 3.0])
                .expect("push it");
            ds.close().expect("close");
        }

        // Reopen (unfinished) and append more data.
        {
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("reopen");
            let ids = ds.wavelengths.push(&[400.0, 700.0]).expect("push wl2");
            assert_eq!(ids[0], 0); // deduped
            assert_eq!(ids[1], 3); // new

            let m = ds
                .measurements
                .push(Some(0.003), None, None, None, None, None, 60_000)
                .expect("push2");
            assert_eq!(m, 1);
            ds.intensities
                .push(m, &ids, &[4.0, 5.0])
                .expect("push it2");
            ds.close().expect("close2");
        }

        // Verify all data.
        let ds = expect_unfinished(&path);
        assert_eq!(ds.wavelengths.read().expect("wl").len(), 4);
        assert_eq!(ds.measurements.read().expect("ms").len(), 2);
        assert_eq!(ds.intensities.read().expect("it").len(), 5);
    }

    #[test]
    fn multiple_open_cycles() {
        let path = tmp();
        {
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
            ds.wavelengths.push(&[400.0]).expect("wl");
            ds.measurements
                .push(None, None, None, None, None, None, 10_000)
                .expect("push");
            ds.close().expect("close");
        }
        for cycle in 1..=3 {
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("reopen");
            ds.measurements
                .push(None, None, None, None, None, None, 10_000)
                .expect("push");
            ds.close().expect("close");
            let ds = expect_unfinished(&path);
            assert_eq!(ds.measurements.read().expect("ms").len(), cycle + 1);
        }
    }

    /* ---------------------------------------------------------------- Query API guards */

    #[test]
    fn is_finished_flag() {
        let path = tmp();
        {
            let ds = unfinished::Dataset::new(&path, &XY).expect("create");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        assert!(!ds.is_finished());

        let path2 = tmp();
        {
            let ds = unfinished::Dataset::new(&path2, &XY).expect("create");
            ds.finish().expect("finish");
        }
        let ds = Dataset::open(&path2).expect("open");
        assert!(ds.is_finished());
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
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("create");
            ds.wavelengths.push(&[400.0]).expect("push wl");
            ds.close().expect("close");
        }
        // new() on existing file should open it for appending.
        {
            let mut ds = unfinished::Dataset::new(&path, &XY).expect("open via new");
            ds.wavelengths.push(&[500.0]).expect("push more");
            ds.close().expect("close");
        }
        let ds = expect_unfinished(&path);
        assert_eq!(ds.wavelengths.read().expect("wl").len(), 2);
    }
}
