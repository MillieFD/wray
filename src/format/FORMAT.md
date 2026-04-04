# `Wray` File Format Specification

**Version:** 1

A `.wr` file stores optical spectroscopy data in three normalised Apache Arrow
IPC tables, alongside a fixed-size binary header and UTF-8 TOML manifest.

---

## 1. Binary Layout

```
Field             Type    Size    Offset   Contents
────────────────  ──────  ──────  ───────  ─────────────────────────────────────
Magic Bytes       UTF-8   4       0        Identifies the format with b"WRAY"
Format Version    u8      1       4        Specifies the schema version
File Type         u8      1       5        Describes the file variant
Manifest Offset   u64 LE  8       6        Byte offset of the TOML manifest
Manifest Length   u64 LE  8       14       Length of the TOML manifest in bytes
Data Segments     Arrow   …       22       One or more Apache Arrow IPC segments
Manifest          UTF-8   M       EOF      File metadata TOML key-value pairs
```

The header is exactly **22 bytes**. The manifest is located at `manifest_offset`
from the start of the file and is `manifest_length` bytes long. All multibyte
integers are **little-endian**.

### File Types

| Value | Name         | Description                                                     |
|-------|--------------|-----------------------------------------------------------------|
| `0`   | `Unfinished` | Arrow IPC **stream** format — supports reading and appending    |
| `1`   | `Finished`   | Arrow IPC **file** format — compression and random-access reads |

Additional file types may be defined in future format versions.

---

## 2. Manifest TOML

```toml
timestamp = 1742345678901234 # Microseconds since UNIX epoch
calibrations = [3, 7]        # Calibration measurement IDs

[units]
x = "mm"
y = "mm"
z = "um"
a = "rad"
```

---

## 3. Arrow Schemas

### Wavelengths

| Column | Arrow Type | Nullable | Description              |
|--------|------------|----------|--------------------------|
| `id`   | `UInt16`   | No       | Unique wavelength ID     |
| `nm`   | `Float32`  | No       | Wavelength in nanometres |

### Measurements

| Column        | Arrow Type | Nullable | Description                                             |
|---------------|------------|----------|---------------------------------------------------------|
| `id`          | `UInt32`   | No       | Auto-incremented measurement ID                         |
| `timestamp`   | `UInt64`   | No       | Microsecond offset from `manifest.timestamp`            |
| `integration` | `UInt32`   | No       | Integration time in microseconds                        |
| `x`           | `Float32`  | Yes      | X coordinate in `manifest.units.x` or `None` if unused. |
| `y`           | `Float32`  | Yes      | Y coordinate in `manifest.units.y` or `None` if unused. |
| `z`           | `Float32`  | Yes      | Z coordinate in `manifest.units.z` or `None` if unused. |
| `a`           | `Float32`  | Yes      | A coordinate in `manifest.units.a` or `None` if unused. |
| `b`           | `Float32`  | Yes      | B coordinate in `manifest.units.b` or `None` if unused. |
| `c`           | `Float32`  | Yes      | C coordinate in `manifest.units.c` or `None` if unused. |

### Intensities

| Column        | Arrow Type | Nullable | Description                   |
|---------------|------------|----------|-------------------------------|
| `measurement` | `UInt32`   | No       | Foreign key → measurements.id |
| `wavelength`  | `UInt16`   | No       | Foreign key → wavelengths.id  |
| `intensity`   | `Float64`  | No       | Spectral intensity value      |

---

## 4. Version Upgrade Policy

Readers **must** check the format version byte (offset 20 B) and reject
any unrecognised version. Minor-version changes should preserve backwards
compatibility. Major-version changes may alter the header layout or schema.

---

## 5. Cross-Language Compatibility

- All Arrow types are standard primitives; no extension types or
  language-specific encodings.
- The manifest is UTF-8 TOML, parseable in every major language.
- The header uses fixed-width LE numbers parsed with `seek` + `read`.
