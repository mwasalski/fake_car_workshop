-- =============================================================
-- CREATE DELTA TABLES - fake_car_workshop
-- Catalog : fake_car_workshop_franchise
-- Schemas : dim  (dimensions)
--           fact (facts)
-- =============================================================



-- =============================================================
-- DIMENSIONS
-- =============================================================


INSERT INTO fake_car_workshop_franchise.dim.dim_customers
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_customers/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.dim.dim_employees
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_employees/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.dim.dim_locations
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_locations/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.dim.dim_products
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_products/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.dim.dim_services
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_services/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.dim.dim_suppliers
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_suppliers/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.dim.dim_vehicles
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files/dim_vehicles/`;


-- =============================================================
-- FACTS
-- Partitioned tables (year/month): appointments, inventory_movements,
--   invoices, payments, sales_transactions, work_orders
-- Non-partitioned tables: all others
-- =============================================================


INSERT INTO fake_car_workshop_franchise.fact.fact_appointments
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_appointments/`;

-- -------------------------------------------------------------


INSERT INTO fake_car_workshop_franchise.fact.fact_customer_feedback
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_customer_feedback/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_employee_schedules
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_employee_schedules/`;

-- -------------------------------------------------------------


INSERT INTO fake_car_workshop_franchise.fact.fact_inventory_movements
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_inventory_movements/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_invoices
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_invoices/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_loyalty_program
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_loyalty_program/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_payments
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_payments/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_purchase_order_items
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_purchase_order_items/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_purchase_orders
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_purchase_orders/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_sales_items
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_sales_items/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_sales_transactions
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_sales_transactions/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_work_order_items
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_work_order_items/`;

-- -------------------------------------------------------------

INSERT INTO fake_car_workshop_franchise.fact.fact_work_orders
SELECT * FROM parquet.`/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files/fact_work_orders/`;
