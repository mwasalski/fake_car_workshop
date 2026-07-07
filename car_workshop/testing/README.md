# Testing & Data Checks

Utilities for inspecting how much data the platform holds and whether it is healthy.

## Files

| file | purpose |
| --- | --- |
| `row_counts.ipynb` | `count(*)` + file count + size for **every** table in the catalog (schemas discovered dynamically), per-schema totals, backfill date-coverage check, rows-per-day trend |
| `test.ipynb` | NULL audit: null count / percentage for every column of every table in a schema |
| `tables_checker.ipynb` | row / distinct counts per table – **manual use only**, contains DELETE cells |

## Usage

- `row_counts.ipynb` – run any time; safe (read-only). Start here after a backfill
  to verify volume and date coverage.
- `test.ipynb` – set `catalog_name` / `schema_name` in the first cell; read-only.
  Chained into `run_all.ipynb` as the optional quality-check section.
- `tables_checker.ipynb` – legacy utility. The `%sql` DELETE cells are destructive
  (the dedup query removes *all* rows of a duplicated `customer_code`, including the
  one you would want to keep) – with per-day ID blocks in the new architecture,
  dedup at this level should not be needed at all.
