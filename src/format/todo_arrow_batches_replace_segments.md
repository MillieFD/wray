### Current Implementation

The project currently uses `Segments` to divide the file:

- Decouples data semantics from its location within the file.
- Different tables (intensities, wavelengths, measurements) written to different segments.
- Footer `manifest.toml` stores segment lengths and offsets to read data.
- New data is appended to an existing file by adding a new segment.
- Appending new data only overwrites the `manifest.toml` footer without touching existing data segments.

### Problem

1. Apache Arrow already supports this functionality via the `RecordBatch` struct.
2. Each `Segment` contains ≥ 1 `RecordBatch`.
3. The `Segment` design therefore adds an unnecessary extra layer of indirection and nesting

### Proposed Solution

Remove the `Segment` struct and use the Arrow `RecordBatch` design only.

- The `wavelengths` table becomes an Arrow dictionary batch
- The `intensities` and `measurements` batches can be interleaved
- The Arrow footer records which batch(es) belong to each table