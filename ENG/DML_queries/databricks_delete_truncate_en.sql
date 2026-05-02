-- =============================================================
-- DELETE & TRUNCATE - fake_car_workshop
-- Catalog : fake_car_workshop_franchise
-- Schemas : dim  (dimensions)
--           fact (facts)
-- =============================================================

-- =============================================================
-- DELETE FROM
-- Removes all rows but preserves Delta history (time travel works)
-- =============================================================

DELETE FROM fake_car_workshop_franchise.dim.dim_customers;
DELETE FROM fake_car_workshop_franchise.dim.dim_employees;
DELETE FROM fake_car_workshop_franchise.dim.dim_locations;
DELETE FROM fake_car_workshop_franchise.dim.dim_products;
DELETE FROM fake_car_workshop_franchise.dim.dim_services;
DELETE FROM fake_car_workshop_franchise.dim.dim_suppliers;
DELETE FROM fake_car_workshop_franchise.dim.dim_vehicles;

-- --- fact ------------------------------------------------------

DELETE FROM fake_car_workshop_franchise.fact.fact_appointments;
DELETE FROM fake_car_workshop_franchise.fact.fact_customer_feedback;
DELETE FROM fake_car_workshop_franchise.fact.fact_employee_schedules;
DELETE FROM fake_car_workshop_franchise.fact.fact_inventory_movements;
DELETE FROM fake_car_workshop_franchise.fact.fact_invoices;
DELETE FROM fake_car_workshop_franchise.fact.fact_loyalty_program;
DELETE FROM fake_car_workshop_franchise.fact.fact_payments;
DELETE FROM fake_car_workshop_franchise.fact.fact_purchase_order_items;
DELETE FROM fake_car_workshop_franchise.fact.fact_purchase_orders;
DELETE FROM fake_car_workshop_franchise.fact.fact_sales_items;
DELETE FROM fake_car_workshop_franchise.fact.fact_sales_transactions;
DELETE FROM fake_car_workshop_franchise.fact.fact_work_order_items;
DELETE FROM fake_car_workshop_franchise.fact.fact_work_orders;

-- =============================================================
-- TRUNCATE
-- Removes all rows and clears Delta history (faster, no time travel)
-- =============================================================

-- --- dim -------------------------------------------------------

TRUNCATE TABLE fake_car_workshop_franchise.dim.dim_customers;
TRUNCATE TABLE fake_car_workshop_franchise.dim.dim_employees;
TRUNCATE TABLE fake_car_workshop_franchise.dim.dim_locations;
TRUNCATE TABLE fake_car_workshop_franchise.dim.dim_products;
TRUNCATE TABLE fake_car_workshop_franchise.dim.dim_services;
TRUNCATE TABLE fake_car_workshop_franchise.dim.dim_suppliers;
TRUNCATE TABLE fake_car_workshop_franchise.dim.dim_vehicles;

-- --- fact ------------------------------------------------------

TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_appointments;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_customer_feedback;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_employee_schedules;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_inventory_movements;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_invoices;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_loyalty_program;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_payments;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_purchase_order_items;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_purchase_orders;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_sales_items;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_sales_transactions;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_work_order_items;
TRUNCATE TABLE fake_car_workshop_franchise.fact.fact_work_orders;



-- DROP
DROP TABLE  fake_car_workshop_franchise.dim.dim_customers;
DROP TABLE fake_car_workshop_franchise.dim.dim_employees;
DROP TABLE  fake_car_workshop_franchise.dim.dim_locations;
DROP TABLE  fake_car_workshop_franchise.dim.dim_products;
DROP TABLE  fake_car_workshop_franchise.dim.dim_services;
DROP TABLE  fake_car_workshop_franchise.dim.dim_suppliers;
DROP TABLE  fake_car_workshop_franchise.dim.dim_vehicles;

-- --- fact ------------------------------------------------------

DROP TABLE  fake_car_workshop_franchise.fact.fact_appointments;
DROP TABLE  fake_car_workshop_franchise.fact.fact_customer_feedback;
DROP TABLE  fake_car_workshop_franchise.fact.fact_employee_schedules;
DROP TABLE  fake_car_workshop_franchise.fact.fact_inventory_movements;
DROP TABLE  fake_car_workshop_franchise.fact.fact_invoices;
DROP TABLE  fake_car_workshop_franchise.fact.fact_loyalty_program;
DROP TABLE  fake_car_workshop_franchise.fact.fact_payments;
DROP TABLE  fake_car_workshop_franchise.fact.fact_purchase_order_items;
DROP TABLE  fake_car_workshop_franchise.fact.fact_purchase_orders;
DROP TABLE  fake_car_workshop_franchise.fact.fact_sales_items;
DROP TABLE  fake_car_workshop_franchise.fact.fact_sales_transactions;
DROP TABLE  fake_car_workshop_franchise.fact.fact_work_order_items;
DROP TABLE  fake_car_workshop_franchise.fact.fact_work_orders;
