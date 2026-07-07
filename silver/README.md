# Silver Layer

Deduplicated, validated, conformed tables built on top of bronze
(`car_workshop.fact.*`). Reference implementation covers the sales pair;
the remaining tables are exercises.

## Files

| file | purpose |
| --- | --- |
| `create_silver.sql` | schema `car_workshop.silver`, checkpoint volume, tables + CHECK constraints + CDF |
| `silver_sales.ipynb` | reference implementation: `sales_transactions` + `sales_items` |

## The pattern (per table)

1. `spark.readStream.table(<bronze>)` – incremental reads, checkpointed on the
   `car_workshop.silver.checkpoints` volume.
2. `foreachBatch`:
   - dedup inside the batch (`dropDuplicates` on the business key),
   - quality rules -> `_reject_reason` column (first failing rule wins),
   - rejects appended to `<table>_quarantine` with `_reject_reason` / `_quarantined_at`,
   - valid rows upserted with an **insert-only `MERGE`** – cross-batch duplicates
     and replayed batches become no-ops (idempotency).
3. `trigger(availableNow=True)` – run after each ingest (or as a job task after
   `autoloader.ipynb`).

## Run order

1. `create_silver.sql` (once; safe to re-run),
2. `silver_sales.ipynb` – transactions **before** items (items validate parent FK
   against `silver.sales_transactions`).

## Exercises (build these yourself, same pattern)

| table | the twist |
| --- | --- |
| `silver.work_orders` | CHECK `completion_date >= reception_date`; dedup |
| `silver.work_order_items` | `0` sentinel -> `NULL` in `service_id` / `product_id` |
| `silver.invoice_payments` | aggregate payments per invoice; derive `paid` / `partial` / `overpaid` / `unpaid` |
| `silver.purchase_orders` | quarantine `status = 'delivered' AND actual_delivery_date > current_date` |
| `silver.appointments` | derived columns: `lead_time_days`, `is_no_show` |
| `silver.customers` | PII: `sha2(national_id, 256)`, mask email / phone |
| `silver.inventory_movements` | validate quantity sign vs `movement_type` |

Extra credit:

- add `_source_file` / `_ingested_at` to bronze in `autoloader.ipynb`
  (`F.col('_metadata.file_path')`, `current_timestamp()`) and carry them into silver,
- rewrite one table as a DLT pipeline with `@dlt.expect_or_drop` and compare,
- `OPTIMIZE` / `VACUUM` the silver tables and inspect `DESCRIBE HISTORY`.

## Notes

- Quarantine tables will be mostly **empty** – the generator produces clean data by
  design. To see rejects, break something on purpose (e.g. delete a few rows from
  `dim_products` and re-run, or insert a manual row with `quantity = 0` into bronze).
- After a full platform reset (truncate + volume cleanup) also remove
  `/Volumes/car_workshop/silver/checkpoints/` – streaming state must match the source.
