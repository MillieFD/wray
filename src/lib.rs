/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

mod database;

// #[cfg(test)]
// mod tests {
//     use super::database::*;
//     use polars::prelude::*;
//     use std::fs;
//     use std::time::{SystemTime, UNIX_EPOCH};
//
//     fn cleanup_test_db(path: &str) {
//         let _ = fs::remove_dir_all(path);
//     }
//
//     #[test]
//     fn test_empty_dataframe() {
//         let df = Spectrometers::empty();
//         assert_eq!(df.height(), 0);
//         assert_eq!(df.width(), 2); // id and wavelengths
//     }
//
//     #[test]
//     fn test_create_database() {
//         let test_path = "test_db_create";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![
//             ("optical_fibre_code", "FC-UV200"),
//             ("target", "Sample A"),
//         ];
//
//         let db = Database::create(test_path, &metadata).expect("Failed to create database");
//
//         // Check that all files exist
//         assert!(db.path().join("spectrometers.parquet").exists());
//         assert!(db.path().join("measurements.parquet").exists());
//         assert!(db.path().join("spectra.parquet").exists());
//         assert!(db.path().join("metadata.toml").exists());
//
//         cleanup_test_db(test_path);
//     }
//
//     #[test]
//     fn test_open_database() {
//         let test_path = "test_db_open";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![("test", "value")];
//         let _db = Database::create(test_path, &metadata).expect("Failed to create database");
//
//         // Now open it
//         let db_opened = Database::open(test_path).expect("Failed to open database");
//         assert_eq!(db_opened.path().to_str().unwrap(), test_path);
//
//         cleanup_test_db(test_path);
//     }
//
//     #[test]
//     fn test_metadata_roundtrip() {
//         let test_path = "test_db_metadata";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![
//             ("optical_fibre_code", "FC-UV200"),
//             ("target", "Sample B"),
//             ("experiment_date", "2026-01-15"),
//         ];
//
//         let db = Database::create(test_path, &metadata).expect("Failed to create database");
//
//         // Read metadata back
//         let read_meta = db.get_metadata().expect("Failed to read metadata");
//
//         assert_eq!(read_meta.len(), 3);
//         assert!(read_meta.contains(&("optical_fibre_code".to_string(), "FC-UV200".to_string())));
//         assert!(read_meta.contains(&("target".to_string(), "Sample B".to_string())));
//         assert!(read_meta.contains(&("experiment_date".to_string(), "2026-01-15".to_string())));
//
//         cleanup_test_db(test_path);
//     }
//
//     #[test]
//     fn test_add_spectrometer() {
//         let test_path = "test_db_spectrometer";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![("test", "value")];
//         let db = Database::create(test_path, &metadata).expect("Failed to create database");
//
//         // Add a spectrometer
//         let wavelengths = vec![350.0, 351.0, 352.0, 353.0, 354.0];
//         let spec_id = db
//             .add_spectrometer(wavelengths.clone())
//             .expect("Failed to add spectrometer");
//
//         assert_eq!(spec_id, 0); // First spectrometer should have ID 0
//
//         // Verify it was written
//         let spec_path = db.path().join("spectrometers.parquet");
//         let df = LazyFrame::scan_parquet(&spec_path, Default::default())
//             .expect("Failed to scan")
//             .collect()
//             .expect("Failed to collect");
//
//         assert_eq!(df.height(), 1);
//         assert_eq!(df.column("id").unwrap().u8().unwrap().get(0).unwrap(), 0);
//
//         cleanup_test_db(test_path);
//     }
//
//     #[test]
//     fn test_add_multiple_spectrometers() {
//         let test_path = "test_db_multi_spec";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![("test", "value")];
//         let db = Database::create(test_path, &metadata).expect("Failed to create database");
//
//         // Add two spectrometers
//         let wavelengths1 = vec![350.0, 351.0, 352.0];
//         let wavelengths2 = vec![400.0, 401.0, 402.0, 403.0];
//
//         let spec_id1 = db.add_spectrometer(wavelengths1).expect("Failed to add spectrometer 1");
//         let spec_id2 = db.add_spectrometer(wavelengths2).expect("Failed to add spectrometer 2");
//
//         assert_eq!(spec_id1, 0);
//         assert_eq!(spec_id2, 1);
//
//         // Verify both were written
//         let spec_path = db.path().join("spectrometers.parquet");
//         let df = LazyFrame::scan_parquet(&spec_path, Default::default())
//             .expect("Failed to scan")
//             .collect()
//             .expect("Failed to collect");
//
//         assert_eq!(df.height(), 2);
//
//         cleanup_test_db(test_path);
//     }
//
//     #[test]
//     fn test_add_measurement_with_spectra() {
//         let test_path = "test_db_measurement";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![("test", "value")];
//         let db = Database::create(test_path, &metadata).expect("Failed to create database");
//
//         // Add a spectrometer first
//         let wavelengths = vec![350.0, 351.0, 352.0];
//         let spec_id = db.add_spectrometer(wavelengths).expect("Failed to add spectrometer");
//
//         // Add a measurement with 2 spectra
//         let timestamp = SystemTime::now()
//             .duration_since(UNIX_EPOCH)
//             .unwrap()
//             .as_secs_f64();
//
//         let spectrum1 = vec![0.1, 0.2, 0.3];
//         let spectrum2 = vec![0.11, 0.21, 0.31];
//
//         let meas_id = db
//             .add_measurement(
//                 0.01,  // x_m
//                 0.02,  // y_m
//                 0.005, // z_m
//                 timestamp,
//                 0.0025,  // interfibre_m
//                 0.1,     // integration_s
//                 spec_id,
//                 vec![spectrum1, spectrum2],
//             )
//             .expect("Failed to add measurement");
//
//         assert_eq!(meas_id, 0); // First measurement
//
//         // Verify measurement was written
//         let meas_path = db.path().join("measurements.parquet");
//         let meas_df = LazyFrame::scan_parquet(&meas_path, Default::default())
//             .expect("Failed to scan")
//             .collect()
//             .expect("Failed to collect");
//
//         assert_eq!(meas_df.height(), 1);
//         assert_eq!(meas_df.column("id").unwrap().u32().unwrap().get(0).unwrap(), 0);
//
//         // Verify spectra were written
//         let spec_path = db.path().join("spectra.parquet");
//         let spec_df = LazyFrame::scan_parquet(&spec_path, Default::default())
//             .expect("Failed to scan")
//             .collect()
//             .expect("Failed to collect");
//
//         assert_eq!(spec_df.height(), 2); // Two spectra
//         assert_eq!(spec_df.column("id").unwrap().u32().unwrap().get(0).unwrap(), 0);
//         assert_eq!(spec_df.column("id").unwrap().u32().unwrap().get(1).unwrap(), 1);
//         assert_eq!(
//             spec_df.column("measurement_id").unwrap().u32().unwrap().get(0).unwrap(),
//             0
//         );
//         assert_eq!(
//             spec_df.column("measurement_id").unwrap().u32().unwrap().get(1).unwrap(),
//             0
//         );
//
//         cleanup_test_db(test_path);
//     }
//
//     #[test]
//     fn test_multiple_measurements() {
//         let test_path = "test_db_multi_meas";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![("test", "value")];
//         let db = Database::create(test_path, &metadata).expect("Failed to create database");
//
//         // Add a spectrometer
//         let wavelengths = vec![350.0, 351.0];
//         let spec_id = db.add_spectrometer(wavelengths).expect("Failed to add spectrometer");
//
//         // Add three measurements
//         let timestamp = SystemTime::now()
//             .duration_since(UNIX_EPOCH)
//             .unwrap()
//             .as_secs_f64();
//
//         for i in 0..3 {
//             let spectrum = vec![0.1 * (i as f64 + 1.0), 0.2 * (i as f64 + 1.0)];
//             let meas_id = db
//                 .add_measurement(
//                     0.01 * (i as f64),
//                     0.02 * (i as f64),
//                     0.005,
//                     timestamp + (i as f64),
//                     0.0025,
//                     0.1,
//                     spec_id,
//                     vec![spectrum],
//                 )
//                 .expect("Failed to add measurement");
//
//             assert_eq!(meas_id, i as u32);
//         }
//
//         // Verify all measurements were written
//         let meas_path = db.path().join("measurements.parquet");
//         let meas_df = LazyFrame::scan_parquet(&meas_path, Default::default())
//             .expect("Failed to scan")
//             .collect()
//             .expect("Failed to collect");
//
//         assert_eq!(meas_df.height(), 3);
//
//         cleanup_test_db(test_path);
//     }
//
//     #[test]
//     fn test_persistence_after_reopen() {
//         let test_path = "test_db_persistence";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![("fibre", "FC-UV200")];
//
//         // Create database and add data
//         {
//             let db = Database::create(test_path, &metadata).expect("Failed to create database");
//             let wavelengths = vec![350.0, 351.0, 352.0];
//             let spec_id = db.add_spectrometer(wavelengths).expect("Failed to add spectrometer");
//
//             let timestamp = 1704067200.0;
//             let spectrum = vec![0.1, 0.2, 0.3];
//             db.add_measurement(0.01, 0.02, 0.005, timestamp, 0.0025, 0.1, spec_id,
// vec![spectrum])                 .expect("Failed to add measurement");
//         } // db goes out of scope
//
//         // Reopen and verify data
//         {
//             let db = Database::open(test_path).expect("Failed to open database");
//
//             // Check metadata
//             let meta = db.get_metadata().expect("Failed to read metadata");
//             assert!(meta.contains(&("fibre".to_string(), "FC-UV200".to_string())));
//
//             // Check spectrometer
//             let spec_path = db.path().join("spectrometers.parquet");
//             let spec_df = LazyFrame::scan_parquet(&spec_path, Default::default())
//                 .expect("Failed to scan")
//                 .collect()
//                 .expect("Failed to collect");
//             assert_eq!(spec_df.height(), 1);
//
//             // Check measurement
//             let meas_path = db.path().join("measurements.parquet");
//             let meas_df = LazyFrame::scan_parquet(&meas_path, Default::default())
//                 .expect("Failed to scan")
//                 .collect()
//                 .expect("Failed to collect");
//             assert_eq!(meas_df.height(), 1);
//
//             // Check spectrum
//             let spec_data_path = db.path().join("spectra.parquet");
//             let spec_data_df = LazyFrame::scan_parquet(&spec_data_path, Default::default())
//                 .expect("Failed to scan")
//                 .collect()
//                 .expect("Failed to collect");
//             assert_eq!(spec_data_df.height(), 1);
//         }
//
//         cleanup_test_db(test_path);
//     }
//
//     #[test]
//     fn test_unit_conversions() {
//         // Test length conversions
//         assert!((mm_to_m(1000.0) - 1.0).abs() < 1e-10);
//         assert!((m_to_mm(1.0) - 1000.0).abs() < 1e-10);
//
//         // Test time conversions
//         assert!((ms_to_s(1000.0) - 1.0).abs() < 1e-10);
//         assert!((s_to_ms(1.0) - 1000.0).abs() < 1e-10);
//
//         // Test wavelength conversions
//         assert!((nm_to_m(1e9) - 1.0).abs() < 1e-10);
//         assert!((m_to_nm(1.0) - 1e9).abs() < 1e-10);
//     }
//
//     #[test]
//     fn test_lazy_reading_memory_efficiency() {
//         let test_path = "test_db_lazy";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![("test", "lazy")];
//         let db = Database::create(test_path, &metadata).expect("Failed to create database");
//
//         // Add spectrometer
//         let wavelengths = vec![350.0, 351.0];
//         let spec_id = db.add_spectrometer(wavelengths).expect("Failed to add spectrometer");
//
//         // Add several measurements (simulating a larger dataset)
//         for i in 0..10 {
//             let spectrum = vec![0.1, 0.2];
//             db.add_measurement(
//                 0.01 * (i as f64),
//                 0.02 * (i as f64),
//                 0.005,
//                 1704067200.0 + (i as f64),
//                 0.0025,
//                 0.1,
//                 spec_id,
//                 vec![spectrum],
//             )
//             .expect("Failed to add measurement");
//         }
//
//         // Test lazy reading without collecting all data
//         let meas_path = db.path().join("measurements.parquet");
//         let lf = LazyFrame::scan_parquet(&meas_path, Default::default())
//             .expect("Failed to scan")
//             .select(&[col("id"), col("x")])
//             .filter(col("x").gt(lit(0.05)));
//
//         // This should only load filtered columns and rows
//         let result = lf.collect().expect("Failed to collect");
//         assert!(result.height() > 0);
//
//         cleanup_test_db(test_path);
//     }
//
//     #[test]
//     fn test_different_spectrometers_different_wavelengths() {
//         let test_path = "test_db_diff_spec";
//         cleanup_test_db(test_path);
//
//         let metadata = vec![("test", "value")];
//         let db = Database::create(test_path, &metadata).expect("Failed to create database");
//
//         // Add two spectrometers with different wavelength arrays
//         let wavelengths1 = vec![350.0, 351.0, 352.0]; // 3 wavelengths
//         let wavelengths2 = vec![400.0, 401.0, 402.0, 403.0, 404.0]; // 5 wavelengths
//
//         let spec_id1 = db.add_spectrometer(wavelengths1).expect("Failed to add spectrometer 1");
//         let spec_id2 = db.add_spectrometer(wavelengths2).expect("Failed to add spectrometer 2");
//
//         // Add measurements with appropriate spectrum lengths
//         let spectrum1 = vec![0.1, 0.2, 0.3]; // 3 values
//         let spectrum2 = vec![0.5, 0.6, 0.7, 0.8, 0.9]; // 5 values
//
//         let meas_id1 = db
//             .add_measurement(0.01, 0.02, 0.005, 1704067200.0, 0.0025, 0.1, spec_id1,
// vec![spectrum1])             .expect("Failed to add measurement 1");
//
//         let meas_id2 = db
//             .add_measurement(0.01, 0.02, 0.005, 1704067201.0, 0.0025, 0.1, spec_id2,
// vec![spectrum2])             .expect("Failed to add measurement 2");
//
//         assert_eq!(meas_id1, 0);
//         assert_eq!(meas_id2, 1);
//
//         // Verify both measurements are stored correctly
//         let meas_path = db.path().join("measurements.parquet");
//         let meas_df = LazyFrame::scan_parquet(&meas_path, Default::default())
//             .expect("Failed to scan")
//             .collect()
//             .expect("Failed to collect");
//
//         assert_eq!(meas_df.height(), 2);
//
//         cleanup_test_db(test_path);
//     }
// }
