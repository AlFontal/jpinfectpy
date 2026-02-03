# Engines

All parsing and transformations are implemented in Polars. Public functions accept `return_type` to decide whether to return Pandas or Polars objects.

Excel ingestion may use Pandas (via `openpyxl`) as a loader step before converting to Polars. This keeps transformations in one Polars pipeline without reimplementing logic twice.

## Why Polars

- Consistent, fast, and memory-efficient transformations.
- Single logic path for wide/long reshaping and merges.

## How to Choose

- Use `return_type="pandas"` if you rely on the Pandas ecosystem.
- Use `return_type="polars"` for large datasets and faster performance.
