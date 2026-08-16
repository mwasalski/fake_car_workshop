-- =============================================================
-- CREATE DELTA TABLES - fake_car_workshop
-- Catalog : car_workshop
-- Schemas : dim  (dimensions)
--           fact (facts)
--
-- All 20 tables are EXTERNAL: each CREATE TABLE carries a LOCATION
-- under abfss://landing@carworkshopadls.dfs.core.windows.net/tables/.
-- PREREQUISITE: storage credential + external location must exist
-- first (infra/create_external_adls.sql, sections 0-2).
--
-- External-table semantics to remember:
--   * DROP TABLE removes only metadata - files stay on ADLS; a later
--     CREATE at the same LOCATION resurrects the table from _delta_log
--     (and then the column list here is ignored).
--   * Full reset therefore needs the paths wiped too:
--     dbutils.fs.rm('/tables/...', recurse=True) via the external
--     location, or 'az storage fs directory delete'.
--   * Paths must not overlap any volume or other table location.
-- =============================================================

CREATE CATALOG IF NOT EXISTS car_workshop;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS car_workshop.dim;
CREATE SCHEMA IF NOT EXISTS car_workshop.fact;


CREATE VOLUME IF NOT EXISTS car_workshop.dim.autoloader_checkpoints;
CREATE VOLUME IF NOT EXISTS car_workshop.fact.autoloader_checkpoints;

-- ============================================================
-- CREATE VOLUMES
-- ============================================================
CREATE VOLUME IF NOT EXISTS  car_workshop.dim.dim_files;
CREATE VOLUME IF NOT EXISTS  car_workshop.fact.fact_files;

-- =============================================================
-- DIMENSIONS
-- =============================================================

CREATE TABLE IF NOT EXISTS car_workshop.dim.dim_customers (
  customer_id              BIGINT,
  customer_code            STRING,
  customer_type            STRING,
  first_name               STRING,
  last_name                STRING,
  company_name             STRING,
  tax_id                   STRING,
  email                    STRING,
  phone                    STRING,
  city                     STRING,
  postal_code              STRING,
  registration_date        DATE,
  preferred_location_id    BIGINT,
  marketing_consent        BOOLEAN
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/dim/dim_customers';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.dim.dim_employees (
  employee_id              BIGINT,
  employee_code            STRING,
  first_name               STRING,
  last_name                STRING,
  national_id              STRING,
  position                 STRING,
  location_id              BIGINT,
  hire_date                DATE,
  termination_date         DATE,
  hourly_rate              DOUBLE,
  is_active                BOOLEAN
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/dim/dim_employees';


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.dim.dim_locations (
  location_id              BIGINT,
  location_code            STRING,
  name                     STRING,
  type                     STRING,
  street                   STRING,
  city                     STRING,
  region                   STRING,
  postal_code              STRING,
  latitude                 DOUBLE,
  longitude                DOUBLE,
  phone                    STRING,
  email                    STRING,
  manager_id               BIGINT,
  number_of_bays           BIGINT,
  area_m2                  BIGINT,
  opening_date             DATE,
  is_active                BOOLEAN
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/dim/dim_locations';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.dim.dim_products (
  product_id               BIGINT,
  product_code             STRING,
  name                     STRING,
  category                 STRING,
  manufacturer             STRING,
  purchase_price_net       DOUBLE,
  sale_price_net           DOUBLE,
  vat_rate                 BIGINT,
  unit                     STRING,
  weight_kg                DOUBLE,
  min_stock_level          BIGINT,
  is_active                BOOLEAN
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/dim/dim_products';


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.dim.dim_services (
  service_id               BIGINT,
  service_code             STRING,
  name                     STRING,
  category                 STRING,
  min_price_net            BIGINT,
  max_price_net            BIGINT,
  estimated_time_min       BIGINT,
  is_active                BOOLEAN
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/dim/dim_services';


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.dim.dim_suppliers (
  supplier_id              BIGINT,
  supplier_code            STRING,
  name                     STRING,
  tax_id                   STRING,
  city                     STRING,
  address                  STRING,
  postal_code              STRING,
  phone                    STRING,
  email                    STRING,
  contact_person           STRING,
  payment_terms_days       BIGINT,
  min_order_value          DOUBLE,
  is_active                BOOLEAN
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/dim/dim_suppliers';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.dim.dim_vehicles (
  vehicle_id                  BIGINT,
  customer_id                 BIGINT,
  make                        STRING,
  model                       STRING,
  year                        BIGINT,
  vin                         STRING,
  registration_number         STRING,
  fuel_type                   STRING,
  engine_displacement         DOUBLE,
  horsepower                  BIGINT,
  color                       STRING,
  mileage_km                  BIGINT,
  first_registration_date     date
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/dim/dim_vehicles';


-- =============================================================
-- FACTS
-- Partitioned tables (year/month): appointments, inventory_movements,
--   invoices, payments, sales_transactions, work_orders
-- Non-partitioned tables: all others
-- =============================================================

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_appointments (
  appointment_id     BIGINT,
  customer_id        BIGINT,
  vehicle_id         BIGINT,
  location_id        BIGINT,
  service_id         BIGINT,
  booking_date       date,
  appointment_date   date,
  status             STRING,
  booking_channel    STRING,
  notes              STRING,
  year               INT,
  month              INT
)
USING DELTA
PARTITIONED BY (year, month)
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_appointments';


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_customer_feedback (
  feedback_id      BIGINT,
  customer_id      BIGINT,
  location_id      BIGINT,
  work_order_id    BIGINT,
  feedback_date    date,
  rating           BIGINT,
  comment          STRING,
  category         STRING,
  channel          STRING
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_customer_feedback';


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_employee_schedules (
  schedule_id      BIGINT,
  employee_id      BIGINT,
  date             date,
  start_hour       BIGINT,
  end_hour         BIGINT,
  shift_type       STRING,
  overtime_hours   BIGINT,
  attendance       STRING
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_employee_schedules';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_inventory_movements (
  movement_id        BIGINT,
  product_id         BIGINT,
  location_id        BIGINT,
  movement_type      STRING,
  quantity           BIGINT,
  movement_date      date,
  source_document    STRING,
  document_number    STRING,
  value_net          DOUBLE,
  notes              STRING,
  year               INT,
  month              INT
)
USING DELTA
PARTITIONED BY (year, month)
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_inventory_movements';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_invoices (
  invoice_id          BIGINT,
  invoice_code        STRING,
  document_type       STRING,
  source_type         STRING,
  source_id           BIGINT,
  customer_id         BIGINT,
  location_id         BIGINT,
  issue_date          date,
  sale_date           date,
  payment_due_date    date,
  value_net           DOUBLE,
  value_vat           DOUBLE,
  value_gross         DOUBLE,
  status              STRING,
  year                INT,
  month               INT
)
USING DELTA
PARTITIONED BY (year, month)
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_invoices';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_loyalty_program (
  loyalty_id       BIGINT,
  customer_id      BIGINT,
  event_date       date,
  event_type       STRING,
  points           BIGINT,
  description      STRING,
  balance_after    BIGINT,
  tier             STRING
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_loyalty_program';


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_payments (
  payment_id          BIGINT,
  invoice_id          BIGINT,
  payment_date        date,
  amount              DOUBLE,
  payment_method      STRING,
  status              STRING,
  transaction_number  STRING,
  year                INT,
  month               INT
)
USING DELTA
PARTITIONED BY (year, month)
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_payments';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_purchase_order_items (
  po_item_id                 BIGINT,
  po_id                      BIGINT,
  product_id                 BIGINT,
  quantity_ordered           BIGINT,
  quantity_delivered         BIGINT,
  unit_price_net             DOUBLE,
  value_net                  DOUBLE
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_purchase_order_items';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_purchase_orders (
  po_id                        BIGINT,
  po_code                      STRING,
  supplier_id                  BIGINT,
  location_id                  BIGINT,
  order_date                   date,
  planned_delivery_date        date,
  actual_delivery_date         date,
  value_net                    DOUBLE,
  value_gross                  DOUBLE,
  status                       STRING,
  year                         INT
)
USING DELTA
PARTITIONED BY (year)
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_purchase_orders';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_sales_items (
  sales_item_id              BIGINT,
  transaction_id             BIGINT,
  product_id                 BIGINT,
  quantity                   BIGINT,
  unit_price_net             DOUBLE,
  discount_percent           BIGINT,
  value_net                  DOUBLE,
  vat_rate                   BIGINT,
  value_gross                DOUBLE
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_sales_items';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_sales_transactions (
  transaction_id       BIGINT,
  transaction_code     STRING,
  location_id          BIGINT,
  customer_id          BIGINT,
  employee_id          BIGINT,
  transaction_date     date,
  payment_method       STRING,
  receipt_number       STRING,
  year                 INT,
  month                INT
)
USING DELTA
PARTITIONED BY (year, month)
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_sales_transactions';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_work_order_items (
  wo_item_id                 BIGINT,
  work_order_id              BIGINT,
  item_type                  STRING,
  service_id                 BIGINT,
  product_id                 BIGINT,
  quantity                   BIGINT,
  unit_price_net             DOUBLE,
  value_net                  DOUBLE,
  vat_rate                   BIGINT,
  value_gross                DOUBLE,
  discount_percent           BIGINT
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_work_order_items';

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_work_orders (
  work_order_id              BIGINT,
  work_order_code            STRING,
  location_id                BIGINT,
  customer_id                BIGINT,
  vehicle_id                 BIGINT,
  mechanic_id                BIGINT,
  reception_date             date,
  completion_date            date,
  status                     STRING,
  mileage_at_reception       BIGINT,
  customer_notes             STRING,
  year                       INT,
  month                      INT
)
USING DELTA
PARTITIONED BY (year, month)
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact/fact_work_orders';

