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
//! 4. [`Open`][3] existing files to add additional data as required.
//! 5. Convert into a read-only format with [`finish`][4] (consume) or [`finish_to`][5] (copy) for
//!    improved random access read performance and reduced file size.
//!
//! # File format
//!
//! ```text
//! [Header 24 B] [Segment 1] [Segment 2] … [Segment N] [Manifest TOML]
//! ```
//!
//! The 24-byte header stores magic bytes `WRAY`, format version, finished flag, and the manifest
//! offset. Each segment holds Arrow IPC stream data. The manifest TOML at the end indexes all
//! segments and stores experiment metadata. On [`finish`][4], segments are consolidated into the
//! Arrow IPC **file** format for random-access reads.
//!
//! [1]: Dataset::new
//! [2]: Dataset::close
//! [3]: Dataset::open
//! [4]: Dataset::finish
//! [5]: Dataset::finish_to

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
pub use self::format::{Config, Manifest, ManifestUnits, Units};
pub use self::records::{IntensityRecord, MeasurementRecord, WavelengthRecord};

/* ---------------------------------------------------------------------------------- Unit Tests */

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::NamedTempFile;

    use super::*;

    const XY: Config = Config {
        x: Some(Units::Mm),
        y: Some(Units::Mm),
        z: None,
        a: None,
    };

    const XYZA: Config = Config {
        x: Some(Units::Mm),
        y: Some(Units::Mm),
        z: Some(Units::Um),
        a: Some(Units::Deg),
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

        // PART 1: Write
        {
            let mut ds = Dataset::new(&path, &XY).expect("create dataset");
            let wl_ids = ds
                .push_wavelengths(&wavelengths_nm)
                .expect("push wavelengths");
            assert_eq!(wl_ids, vec![0, 1, 2, 3]);

            let x = Length::new::<millimeter>(1.0);
            let y = Length::new::<millimeter>(2.0);
            let integration = Time::new::<millisecond>(100.0);
            let id = ds
                .push(Some(x), Some(y), None, None, integration)
                .expect("push");
            assert_eq!(id, 0);

            let intensities = vec![0.1, 0.2, 0.3, 0.4];
            ds.push_intensities(id, &wl_ids, &intensities)
                .expect("push intensities");

            ds.close().expect("close");
        }

        // PART 2: Read
        {
            let ds = Dataset::open(&path).expect("open dataset");
            assert!(!ds.manifest().finished);
            assert_eq!(ds.manifest().version, 1u32);

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
            ds.push_wavelengths(&[500.0]).expect("push wl");
            let integration = Time::new::<millisecond>(50.0);
            ds.push(None, None, None, None, integration).expect("push");
            // No explicit close — rely on Drop
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
            let integration = Time::new::<millisecond>(10.0);
            // Push with no coordinates at all
            ds.push(None, None, None, None, integration)
                .expect("push none");
            // Push with only x
            let x = Length::new::<millimeter>(5.0);
            ds.push(Some(x), None, None, None, integration)
                .expect("push x");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        let ms = ds.read_measurements().expect("read");
        assert_eq!(ms.len(), 2);
        assert!(ms[0].x.is_none());
        assert!(ms[0].y.is_none());
        assert!(ms[1].x.is_some());
        assert!((ms[1].x.unwrap() - 5.0).abs() < 1e-3);
    }

    /* ----------------------------------------------------------------------------- Calibration */

    #[test]
    fn calibration_marker() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            let integration = Time::new::<millisecond>(10.0);
            let id = ds.push(None, None, None, None, integration).expect("push");
            ds.calibration(id);
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        assert_eq!(ds.manifest().calibrations, vec![0]);
    }

    /* ----------------------------------------------------------------------------- Finish flag */

    #[test]
    fn finish_sets_flag() {
        let path = tmp();
        {
            let ds = Dataset::new(&path, &XY).expect("create");
            ds.finish().expect("finish");
        }
        let ds = Dataset::open(&path).expect("open");
        assert!(ds.manifest().finished);
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
        assert_eq!(ds.manifest().units.x, Some(Units::Mm));
        assert_eq!(ds.manifest().units.y, Some(Units::Mm));
        assert_eq!(ds.manifest().units.z, Some(Units::Um));
        assert_eq!(ds.manifest().units.a, Some(Units::Deg));
    }

    /* ---------------------------------------------------------------- Wavelength deduplication */

    #[test]
    fn wavelength_dedup() {
        let path = tmp();
        let mut ds = Dataset::new(&path, &XY).expect("create");
        let ids1 = ds.push_wavelengths(&[400.0, 500.0]).expect("first push");
        let ids2 = ds.push_wavelengths(&[400.0, 600.0]).expect("second push");
        assert_eq!(ids1, vec![0, 1]);
        assert_eq!(ids2, vec![0, 2]); // 400.0 reused, 600.0 is new
    }

    /* ------------------------------------------------------------------------- Commit snapshot */

    #[test]
    fn commit_writes_readable_file() {
        let path = tmp();
        let mut ds = Dataset::new(&path, &XY).expect("create");
        ds.push_wavelengths(&[450.0]).expect("push wl");
        let integration = Time::new::<millisecond>(20.0);
        ds.push(None, None, None, None, integration).expect("push");
        ds.commit().expect("commit");

        // File should be readable mid-experiment
        let snap = Dataset::open(&path).expect("open snapshot");
        assert_eq!(snap.read_wavelengths().expect("read").len(), 1);

        // Can still push more data after committing
        ds.push_wavelengths(&[550.0]).expect("push more");
        ds.close().expect("close");

        let final_ds = Dataset::open(&path).expect("open final");
        assert_eq!(final_ds.read_wavelengths().expect("read").len(), 2);
    }

    /* ------------------------------------------- Full coordinates (x, y, z, a) */

    #[test]
    fn full_coordinates() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XYZA).expect("create");
            let x = Length::new::<millimeter>(1.0);
            let y = Length::new::<millimeter>(2.0);
            let z = Length::new::<millimeter>(0.003); // 3 µm = 0.003 mm; stored as µm
            let a = Angle::new::<degree>(45.0);
            let integration = Time::new::<millisecond>(100.0);
            ds.push(Some(x), Some(y), Some(z), Some(a), integration)
                .expect("push");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        let ms = ds.read_measurements().expect("read");
        assert_eq!(ms.len(), 1);
        assert!((ms[0].x.expect("x") - 1.0).abs() < 0.01);
        assert!((ms[0].y.expect("y") - 2.0).abs() < 0.01);
        // z is stored in µm: 0.003 mm = 3 µm
        assert!((ms[0].z.expect("z") - 3.0).abs() < 0.01);
        assert!((ms[0].a.expect("a") - 45.0).abs() < 0.01);
    }

    /* ------------------------------------------- Relative timestamps */

    #[test]
    fn timestamps_are_relative() {
        let path = tmp();
        {
            let mut ds = Dataset::new(&path, &XY).expect("create");
            let integration = Time::new::<millisecond>(10.0);
            ds.push(None, None, None, None, integration).expect("push");
            ds.close().expect("close");
        }
        let ds = Dataset::open(&path).expect("open");
        let ms = ds.read_measurements().expect("read");
        // Timestamp should be a small offset (< 1 second) from init, not a UNIX epoch
        assert!(ms[0].timestamp < 1_000_000); // < 1 second in µs
    }
}
