# wray

Standardised data storage for diffuse reflectance spectroscopy imaging, written in Rust.

`wray` stores spectroscopy data in a structured directory of [Apache Arrow IPC](https://arrow.apache.org/docs/format/IPC.html) files, with ZSTD compression.
It provides a simple API for recording wavelengths, spatial measurements, and per-wavelength intensity readings.

## Data Architecture

A `wray` database is a directory containing three Arrow IPC stream files:

| File | Columns | Description |
|---|---|---|
| `wavelengths.arrow` | `id` (UInt32), `nm` (Float64) | Wavelength lookup table, stored in nanometres |
| `measurements.arrow` | `id` (UInt32), `timestamp` (Timestamp), `x`/`y`/`z`/`a` (Float64), `integration` (Duration) | Spatial measurement positions and integration times |
| `intensities.arrow` | `measurement` (UInt32), `wavelength` (UInt32), `intensity` (Float64) | Per-wavelength intensity readings, keyed to `measurements` and `wavelengths` by ID |

Spatial position columns (`x`, `y`, `z`, `a`) are stored in micrometres and are enabled individually via [Cargo features](#features).

## Features

| Feature | Default | Description |
|---|---|---|
| `x` | ✓ | Enable the X-axis position column in `measurements` |
| `y` | ✓ | Enable the Y-axis position column in `measurements` |
| `z` | | Enable the Z-axis position column in `measurements` |
| `a` | | Enable the A-axis (rotation) column in `measurements` |

## Installation

Add `wray` to your `Cargo.toml`:

```toml
[dependencies]
wray = "0.1"
```

To enable additional axes, specify the features you need:

```toml
[dependencies]
wray = { version = "0.1", features = ["x", "y", "z"] }
```

Because `wray` uses nightly-only Rust features, a `nightly` toolchain is required.
The included [`rust-toolchain.toml`](rust-toolchain.toml) pins the toolchain automatically when building inside the repository.

## Usage

```rust
use uom::si::f64::{Length, Time};
use uom::si::length::micrometer;
use uom::si::time::microsecond;
use wray::Database;

fn main() -> Result<(), wray::Error> {
    // Open (or create) a database directory
    let mut db = Database::new("my_scan.wray")?;

    // Register the wavelengths used by your spectrometer
    let wavelength_ids = db.wavelengths.push(vec![400.0, 500.0, 600.0, 700.0])?;
    db.wavelengths.commit()?;

    // Record a measurement at a specific XY position with a 10,000 µs (10 ms) integration time
    let measurement_id = db.measurements.push(
        Length::new::<micrometer>(0.0),   // x
        Length::new::<micrometer>(0.0),   // y
        Time::new::<microsecond>(10_000.0),
    );
    db.measurements.commit()?;

    // Store the intensity readings for that measurement
    let intensities = vec![0.82, 0.74, 0.61, 0.55];
    db.intensities.push(measurement_id, &wavelength_ids, intensities);
    db.intensities.commit()?;

    Ok(())
}
```

## License

`wray` is distributed under the [BSD 3-Clause License](LICENSE).
