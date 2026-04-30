-- =============================================================
-- CREATE DELTA TABLES - fake_car_workshop
-- Catalog : fake_car_workshop_franchise
-- Schemas : dim  (dimensions)
--           fact (facts)
-- =============================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS fake_car_workshop_franchise.dim;
CREATE SCHEMA IF NOT EXISTS fake_car_workshop_franchise.fact;

-- =============================================================
-- DIMENSIONS
-- =============================================================

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.dim.dim_customers (
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
  registration_date        TIMESTAMP,
  preferred_location_id    BIGINT,
  marketing_consent        BOOLEAN
)
USING DELTA;

INSERT INTO fake_car_workshop_franchise.dim.dim_customers
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_customers/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.dim.dim_employees (
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
USING DELTA;

INSERT INTO fake_car_workshop_franchise.dim.dim_employees
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_employees/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.dim.dim_locations (
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
USING DELTA;

INSERT INTO fake_car_workshop_franchise.dim.dim_locations
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_locations/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.dim.dim_products (
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
USING DELTA;

INSERT INTO fake_car_workshop_franchise.dim.dim_products
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_products/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.dim.dim_services (
  service_id               BIGINT,
  service_code             STRING,
  name                     STRING,
  category                 STRING,
  min_price_net            BIGINT,
  max_price_net            BIGINT,
  estimated_time_min       BIGINT,
  is_active                BOOLEAN
)
USING DELTA;

INSERT INTO fake_car_workshop_franchise.dim.dim_services
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_services/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.dim.dim_suppliers (
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
USING DELTA;

INSERT INTO fake_car_workshop_franchise.dim.dim_suppliers
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_suppliers/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.dim.dim_vehicles (
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
  first_registration_date     TIMESTAMP
)
USING DELTA;

INSERT INTO fake_car_workshop_franchise.dim.dim_vehicles
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_vehicles/`;


-- =============================================================
-- FACTS
-- Partitioned tables (year/month): appointments, inventory_movements,
--   invoices, payments, sales_transactions, work_orders
-- Non-partitioned tables: all others
-- =============================================================

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_appointments (
  appointment_id     BIGINT,
  customer_id        BIGINT,
  vehicle_id         BIGINT,
  location_id        BIGINT,
  service_id         BIGINT,
  booking_date       TIMESTAMP,
  appointment_date   TIMESTAMP,
  status             STRING,
  booking_channel    STRING,
  notes              STRING,
  year               INT,
  month              INT
)
USING DELTA
PARTITIONED BY (year, month);

INSERT INTO fake_car_workshop_franchise.fact.fact_appointments
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_appointments/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_customer_feedback (
  feedback_id      BIGINT,
  customer_id      BIGINT,
  location_id      BIGINT,
  work_order_id    BIGINT,
  feedback_date    TIMESTAMP,
  rating           BIGINT,
  comment          STRING,
  category         STRING,
  channel          STRING
)
USING DELTA;

INSERT INTO fake_car_workshop_franchise.fact.fact_customer_feedback
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_customer_feedback/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_employee_schedules (
  schedule_id      BIGINT,
  employee_id      BIGINT,
  date             TIMESTAMP,
  start_hour       BIGINT,
  end_hour         BIGINT,
  shift_type       STRING,
  overtime_hours   BIGINT,
  attendance       STRING
)
USING DELTA;

INSERT INTO fake_car_workshop_franchise.fact.fact_employee_schedules
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_employee_schedules/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_inventory_movements (
  movement_id        BIGINT,
  product_id         BIGINT,
  location_id        BIGINT,
  movement_type      STRING,
  quantity           BIGINT,
  movement_date      TIMESTAMP,
  source_document    STRING,
  document_number    STRING,
  value_net          DOUBLE,
  notes              STRING,
  year               INT,
  month              INT
)
USING DELTA
PARTITIONED BY (year, month);

INSERT INTO fake_car_workshop_franchise.fact.fact_inventory_movements
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_inventory_movements/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_invoices (
  invoice_id          BIGINT,
  invoice_code        STRING,
  document_type       STRING,
  source_type         STRING,
  source_id           BIGINT,
  customer_id         BIGINT,
  location_id         BIGINT,
  issue_date          TIMESTAMP,
  sale_date           TIMESTAMP,
  payment_due_date    TIMESTAMP,
  value_net           DOUBLE,
  value_vat           DOUBLE,
  value_gross         DOUBLE,
  status              STRING,
  year                INT,
  month               INT
)
USING DELTA
PARTITIONED BY (year, month);

INSERT INTO fake_car_workshop_franchise.fact.fact_invoices
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_invoices/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_loyalty_program (
  loyalty_id       BIGINT,
  customer_id      BIGINT,
  event_date       TIMESTAMP,
  event_type       STRING,
  points           BIGINT,
  description      STRING,
  balance_after    BIGINT,
  tier             STRING
)
USING DELTA;

INSERT INTO fake_car_workshop_franchise.fact.fact_loyalty_program
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_loyalty_program/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_payments (
  payment_id          BIGINT,
  invoice_id          BIGINT,
  payment_date        TIMESTAMP,
  amount              DOUBLE,
  payment_method      STRING,
  status              STRING,
  transaction_number  STRING,
  year                INT,
  month               INT
)
USING DELTA
PARTITIONED BY (year, month);

INSERT INTO fake_car_workshop_franchise.fact.fact_payments
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_payments/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_purchase_order_items (
  po_item_id                 BIGINT,
  po_id                      BIGINT,
  product_id                 BIGINT,
  quantity_ordered           BIGINT,
  quantity_delivered         BIGINT,
  unit_price_net             DOUBLE,
  value_net                  DOUBLE
)
USING DELTA;

INSERT INTO fake_car_workshop_franchise.fact.fact_purchase_order_items
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_purchase_order_items/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_purchase_orders (
  po_id                        BIGINT,
  po_code                      STRING,
  supplier_id                  BIGINT,
  location_id                  BIGINT,
  order_date                   TIMESTAMP,
  planned_delivery_date        TIMESTAMP,
  actual_delivery_date         TIMESTAMP,
  value_net                    DOUBLE,
  value_gross                  DOUBLE,
  status                       STRING,
  year                         INT
)
USING DELTA;

INSERT INTO fake_car_workshop_franchise.fact.fact_purchase_orders
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_purchase_orders/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_sales_items (
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
USING DELTA;

INSERT INTO fake_car_workshop_franchise.fact.fact_sales_items
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_sales_items/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_sales_transactions (
  transaction_id       BIGINT,
  transaction_code     STRING,
  location_id          BIGINT,
  customer_id          BIGINT,
  employee_id          BIGINT,
  transaction_date     TIMESTAMP,
  payment_method       STRING,
  receipt_number       STRING,
  year                 INT,
  month                INT
)
USING DELTA
PARTITIONED BY (year, month);

INSERT INTO fake_car_workshop_franchise.fact.fact_sales_transactions
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_sales_transactions/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_work_order_items (
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
USING DELTA;

INSERT INTO fake_car_workshop_franchise.fact.fact_work_order_items
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_work_order_items/`;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise.fact.fact_work_orders (
  work_order_id              BIGINT,
  work_order_code            STRING,
  location_id                BIGINT,
  customer_id                BIGINT,
  vehicle_id                 BIGINT,
  mechanic_id                BIGINT,
  reception_date             TIMESTAMP,
  completion_date            TIMESTAMP,
  status                     STRING,
  mileage_at_reception       BIGINT,
  customer_notes             STRING,
  year                       INT,
  month                      INT
)
USING DELTA
PARTITIONED BY (year, month);

INSERT INTO fake_car_workshop_franchise.fact.fact_work_orders
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_work_orders/`;
