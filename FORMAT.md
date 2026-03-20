# `.wray` File Format Specification

**Version:** 1

A `.wray` file stores spatially located optical spectroscopy data in three
normalised Apache Arrow IPC tables, preceded by a fixed-size binary header
and a UTF-8 TOML manifest.

---

## 1. Binary Layout

```
Offset  Size     Contents
──────  ───────  ──────────────────────────────────────
0       4        Magic bytes: b"WRAY"
4       4        Format version: u32 LE = 1
8       8        manifest_len: u64 LE
16      8        wavelengths_len: u64 LE
24      8        measurements_len: u64 LE
32      8        intensities_len: u64 LE
40      M        Manifest (UTF-8 TOML, manifest_len bytes)
40+M    W        Wavelengths Arrow IPC stream (wavelengths_len bytes)
40+M+W  S        Measurements Arrow IPC stream (measurements_len bytes)
40+M+W+S I      Intensities Arrow IPC stream (intensities_len bytes)
```

The header is exactly **40 bytes**. Section byte ranges are derived sequentially:

| Section       | Start          | Length             |
|---------------|----------------|--------------------|
| Manifest      | 40             | `manifest_len`     |
| Wavelengths   | 40 + M         | `wavelengths_len`  |
| Measurements  | 40 + M + W     | `measurements_len` |
| Intensities   | 40 + M + W + S | `intensities_len`  |

All multi-byte integers are **little-endian**.

---

## 2. Manifest (TOML)

```toml
version = 1.0
timestamp = 1742345678901234   # Absolute UNIX epoch microseconds (i64)
calibrations = [3, 7]          # Measurement IDs flagged as calibrations
finished = false               # True only when explicitly finalised

[units]
x = "mm"                       # Valid: "nm", "um", "mm", "m"
y = "mm"
z = "um"
a = "deg"                      # Valid: "deg", "rad"
```

| Key               | Type       | Description                                              |
|-------------------|------------|----------------------------------------------------------|
| `version`         | `f64`      | Format version. Readers must reject unknown versions.    |
| `timestamp`       | `i64`      | UNIX epoch in microseconds when the dataset was created. |
| `calibrations`    | `[u32]`    | Measurement IDs that are calibration measurements.       |
| `finished`        | `bool`     | Whether the experiment was explicitly finalised.         |
| `units.x`         | `string?`  | Storage unit for x coordinate axis (omitted if unused).  |
| `units.y`         | `string?`  | Storage unit for y coordinate axis.                      |
| `units.z`         | `string?`  | Storage unit for z coordinate axis.                      |
| `units.a`         | `string?`  | Storage unit for angle coordinate axis.                  |

---

## 3. Arrow Schemas

All Arrow IPC sections use **stream format** with **ZSTD compression** per
record batch. Each stream ends with the standard 8-byte EOS sentinel
(`0xFFFFFFFF 0x00000000`).

### Wavelengths

| Column | Arrow Type | Nullable | Description                 |
|--------|------------|----------|-----------------------------|
| `id`   | `UInt16`   | No       | Unique wavelength ID        |
| `nm`   | `Float64`  | No       | Wavelength in nanometres    |

### Measurements

| Column        | Arrow Type | Nullable | Description                                            |
|---------------|------------|----------|--------------------------------------------------------|
| `id`          | `UInt32`   | No       | Auto-incremented measurement ID                        |
| `timestamp`   | `UInt64`   | No       | Microsecond offset from `manifest.timestamp`           |
| `x`           | `Float32`  | Yes      | X coordinate in `units.x` (null if axis unused)        |
| `y`           | `Float32`  | Yes      | Y coordinate in `units.y`                              |
| `z`           | `Float32`  | Yes      | Z coordinate in `units.z`                              |
| `a`           | `Float32`  | Yes      | Angle coordinate in `units.a`                          |
| `integration` | `UInt64`   | No       | Integration time in microseconds                       |

### Intensities

| Column        | Arrow Type | Nullable | Description                            |
|---------------|------------|----------|----------------------------------------|
| `measurement` | `UInt32`   | No       | Foreign key → measurements.id          |
| `wavelength`  | `UInt16`   | No       | Foreign key → wavelengths.id           |
| `intensity`   | `Float64`  | No       | Spectral intensity value               |

---

## 4. Version Upgrade Policy

Readers **must** check `manifest.version` and reject versions they do not
understand. New minor-version additions (new optional manifest keys, new
nullable columns) should preserve backwards compatibility. Major-version
changes may alter the header layout or schema.

---

## 5. Cross-Language Compatibility

- All Arrow types are standard primitives — no extension types or
  language-specific encodings.
- The manifest is UTF-8 TOML, parseable in every major language.
- The 40-byte header uses fixed-width little-endian integers, trivial
  to parse with `seek` + `read`.
- All section offsets are in the header — no seek-and-scan required.
