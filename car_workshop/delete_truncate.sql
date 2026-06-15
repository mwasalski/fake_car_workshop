-- =============================================================
-- DELETE & TRUNCATE - fake_car_workshop
-- Catalog : car_workshop
-- Schemas : dim  (dimensions)
--           fact (facts)
-- =============================================================

-- =============================================================
-- DELETE FROM
-- Removes all rows but preserves Delta history (time travel works)
-- =============================================================

DELETE FROM car_workshop.dim.dim_customers;
DELETE FROM car_workshop.dim.dim_employees;
DELETE FROM car_workshop.dim.dim_locations;
DELETE FROM car_workshop.dim.dim_products;
DELETE FROM car_workshop.dim.dim_services;
DELETE FROM car_workshop.dim.dim_suppliers;
DELETE FROM car_workshop.dim.dim_vehicles;

-- --- fact ------------------------------------------------------

DELETE FROM car_workshop.fact.fact_appointments;
DELETE FROM car_workshop.fact.fact_customer_feedback;
DELETE FROM car_workshop.fact.fact_employee_schedules;
DELETE FROM car_workshop.fact.fact_inventory_movements;
DELETE FROM car_workshop.fact.fact_invoices;
DELETE FROM car_workshop.fact.fact_loyalty_program;
DELETE FROM car_workshop.fact.fact_payments;
DELETE FROM car_workshop.fact.fact_purchase_order_items;
DELETE FROM car_workshop.fact.fact_purchase_orders;
DELETE FROM car_workshop.fact.fact_sales_items;
DELETE FROM car_workshop.fact.fact_sales_transactions;
DELETE FROM car_workshop.fact.fact_work_order_items;
DELETE FROM car_workshop.fact.fact_work_orders;

-- =============================================================
-- TRUNCATE
-- Removes all rows and clears Delta history (faster, no time travel)
-- =============================================================

-- --- dim -------------------------------------------------------

TRUNCATE TABLE car_workshop.dim.dim_customers;
TRUNCATE TABLE car_workshop.dim.dim_employees;
TRUNCATE TABLE car_workshop.dim.dim_locations;
TRUNCATE TABLE car_workshop.dim.dim_products;
TRUNCATE TABLE car_workshop.dim.dim_services;
TRUNCATE TABLE car_workshop.dim.dim_suppliers;
TRUNCATE TABLE car_workshop.dim.dim_vehicles;

-- --- fact ------------------------------------------------------

TRUNCATE TABLE car_workshop.fact.fact_appointments;
TRUNCATE TABLE car_workshop.fact.fact_customer_feedback;
TRUNCATE TABLE car_workshop.fact.fact_employee_schedules;
TRUNCATE TABLE car_workshop.fact.fact_inventory_movements;
TRUNCATE TABLE car_workshop.fact.fact_invoices;
TRUNCATE TABLE car_workshop.fact.fact_loyalty_program;
TRUNCATE TABLE car_workshop.fact.fact_payments;
TRUNCATE TABLE car_workshop.fact.fact_purchase_order_items;
TRUNCATE TABLE car_workshop.fact.fact_purchase_orders;
TRUNCATE TABLE car_workshop.fact.fact_sales_items;
TRUNCATE TABLE car_workshop.fact.fact_sales_transactions;
TRUNCATE TABLE car_workshop.fact.fact_work_order_items;
TRUNCATE TABLE car_workshop.fact.fact_work_orders;



-- DROP
DROP TABLE  car_workshop.dim.dim_customers;
DROP TABLE car_workshop.dim.dim_employees;
DROP TABLE  car_workshop.dim.dim_locations;
DROP TABLE  car_workshop.dim.dim_products;
DROP TABLE  car_workshop.dim.dim_services;
DROP TABLE  car_workshop.dim.dim_suppliers;
DROP TABLE  car_workshop.dim.dim_vehicles;

-- --- fact ------------------------------------------------------

DROP TABLE  car_workshop.fact.fact_appointments;
DROP TABLE  car_workshop.fact.fact_customer_feedback;
DROP TABLE  car_workshop.fact.fact_employee_schedules;
DROP TABLE  car_workshop.fact.fact_inventory_movements;
DROP TABLE  car_workshop.fact.fact_invoices;
DROP TABLE  car_workshop.fact.fact_loyalty_program;
DROP TABLE  car_workshop.fact.fact_payments;
DROP TABLE  car_workshop.fact.fact_purchase_order_items;
DROP TABLE  car_workshop.fact.fact_purchase_orders;
DROP TABLE  car_workshop.fact.fact_sales_items;
DROP TABLE  car_workshop.fact.fact_sales_transactions;
DROP TABLE  car_workshop.fact.fact_work_order_items;
DROP TABLE  car_workshop.fact.fact_work_orders;
