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
4       8        manifest_offset: u64 LE
12      8        manifest_len: u64 LE
20      1        Format version: u8 = 1
21      1        File type: u8 (0 = Unfinished, 1 = Finished)
22      …        Arrow IPC segments (one or more)
22+…    M        Manifest (UTF-8 TOML, manifest_len bytes)
```

The header is exactly **22 bytes**. The manifest is located at
`manifest_offset` from the start of the file and is `manifest_len` bytes long.

| Field             | Offset | Size | Description                                        |
|-------------------|--------|------|----------------------------------------------------|
| Magic bytes       | 0      | 4 B  | `b"WRAY"` — identifies the file type              |
| `manifest_offset` | 4      | 8 B  | Byte offset of the TOML manifest from file start   |
| `manifest_len`    | 12     | 8 B  | Length of the TOML manifest in bytes               |
| Format version    | 20     | 1 B  | `1` — readers must reject unknown versions         |
| File type         | 21     | 1 B  | `0` = Unfinished (stream), `1` = Finished (file)  |

All multi-byte integers are **little-endian**.

### File Types

| Value | Name         | Description                                                |
|-------|--------------|------------------------------------------------------------|
| `0`   | `Unfinished` | Arrow IPC **stream** format — supports reading and appending |
| `1`   | `Finished`   | Arrow IPC **file** format — compression and random-access reads |

Additional file types may be defined in future format versions.

---

## 2. Manifest (TOML)

```toml
version = 1.0
timestamp = 1742345678901234   # Absolute UNIX epoch microseconds (i64)
calibrations = [3, 7]          # Measurement IDs flagged as calibrations

[units]
x = "mm"                       # Valid: "nm", "um", "mm", "m"
y = "mm"
z = "um"
a = "deg"                      # Valid: "deg", "rad"
```

| Key               | Type       | Description                                              |
|-------------------|------------|----------------------------------------------------------|
| `timestamp`       | `i64`      | UNIX epoch in microseconds when the dataset was created. |
| `calibrations`    | `[u32]`    | Measurement IDs that are calibration measurements.       |
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

Readers **must** check the format version byte (offset 20) and reject versions
they do not understand. New minor-version additions (new optional manifest
keys, new nullable columns) should preserve backwards compatibility.
Major-version changes may alter the header layout or schema.

---

## 5. Cross-Language Compatibility

- All Arrow types are standard primitives — no extension types or
  language-specific encodings.
- The manifest is UTF-8 TOML, parseable in every major language.
- The 22-byte header uses fixed-width little-endian integers, trivial
  to parse with `seek` + `read`.
- All section offsets are in the header — no seek-and-scan required.
