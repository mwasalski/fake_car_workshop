-- =============================================================
-- SILVER LAYER - fake_car_workshop
-- Schema  : car_workshop.silver
-- Pattern : bronze (car_workshop.fact.*) -> deduplicated, validated,
--           conformed silver tables + quarantine tables for rejects
--
-- Safe to re-run: CREATE uses IF NOT EXISTS, constraints are guarded
-- by DROP CONSTRAINT IF EXISTS.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS car_workshop.silver;

-- streaming checkpoints for bronze -> silver jobs
CREATE VOLUME IF NOT EXISTS car_workshop.silver.checkpoints;

-- -------------------------------------------------------------
-- sales_transactions
--   * deduplicated by transaction_id
--   * FK-validated against dim_locations / dim_employees
--   * year/month dropped (redundant with transaction_date)
--   * liquid clustering instead of hive-style partitioning
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.silver.sales_transactions (
  transaction_id   BIGINT NOT NULL,
  transaction_code STRING,
  location_id      BIGINT,
  customer_id      BIGINT COMMENT 'NULL = walk-in customer',
  employee_id      BIGINT,
  transaction_date DATE,
  payment_method   STRING,
  receipt_number   STRING,
  _processed_at    TIMESTAMP
)
USING DELTA
CLUSTER BY (location_id, transaction_date)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE car_workshop.silver.sales_transactions DROP CONSTRAINT IF EXISTS valid_payment_method;
ALTER TABLE car_workshop.silver.sales_transactions ADD CONSTRAINT valid_payment_method
  CHECK (payment_method IN ('cash', 'card', 'bank_transfer', 'BLIK', 'leasing', 'instalments'));

CREATE TABLE IF NOT EXISTS car_workshop.silver.sales_transactions_quarantine (
  transaction_id   BIGINT,
  transaction_code STRING,
  location_id      BIGINT,
  customer_id      BIGINT,
  employee_id      BIGINT,
  transaction_date DATE,
  payment_method   STRING,
  receipt_number   STRING,
  year             INT,
  month            INT,
  _reject_reason   STRING,
  _quarantined_at  TIMESTAMP
)
USING DELTA;

-- -------------------------------------------------------------
-- sales_items
--   * deduplicated by sales_item_id
--   * FK-validated against dim_products and silver.sales_transactions
--   * enriched: product_category, margin_net
--   * gross/net consistency checked (rejects -> quarantine)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.silver.sales_items (
  sales_item_id    BIGINT NOT NULL,
  transaction_id   BIGINT,
  product_id       BIGINT,
  product_category STRING,
  quantity         BIGINT,
  unit_price_net   DOUBLE,
  discount_percent BIGINT,
  value_net        DOUBLE,
  vat_rate         BIGINT,
  value_gross      DOUBLE,
  margin_net       DOUBLE COMMENT 'value_net - purchase cost (dim_products.purchase_price_net * quantity)',
  _processed_at    TIMESTAMP
)
USING DELTA
CLUSTER BY (transaction_id)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE car_workshop.silver.sales_items DROP CONSTRAINT IF EXISTS positive_quantity;
ALTER TABLE car_workshop.silver.sales_items ADD CONSTRAINT positive_quantity
  CHECK (quantity > 0);

ALTER TABLE car_workshop.silver.sales_items DROP CONSTRAINT IF EXISTS non_negative_value;
ALTER TABLE car_workshop.silver.sales_items ADD CONSTRAINT non_negative_value
  CHECK (value_net >= 0);

CREATE TABLE IF NOT EXISTS car_workshop.silver.sales_items_quarantine (
  sales_item_id    BIGINT,
  transaction_id   BIGINT,
  product_id       BIGINT,
  quantity         BIGINT,
  unit_price_net   DOUBLE,
  discount_percent BIGINT,
  value_net        DOUBLE,
  vat_rate         BIGINT,
  value_gross      DOUBLE,
  _reject_reason   STRING,
  _quarantined_at  TIMESTAMP
)
USING DELTA;

-- =============================================================
-- TODO (exercises) - same pattern, your turn:
--
--   silver.work_orders            dedup + CHECK (completion_date >= reception_date)
--   silver.work_order_items       0-sentinel -> NULL in service_id / product_id
--   silver.invoice_payments       payments aggregated per invoice, paid vs gross,
--                                 derived status: paid / partial / overpaid / unpaid
--   silver.purchase_orders        flag: status = 'delivered' AND actual_delivery_date > current_date
--   silver.appointments           derived: lead_time_days, is_no_show
--   silver.customers              PII: sha2(national_id), masked email / phone
--   silver.inventory_movements    sign-of-quantity vs movement_type validation
-- =============================================================
