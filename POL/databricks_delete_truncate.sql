-- =============================================================
-- DELETE & TRUNCATE - fake_car_workshop
-- Catalog : fake_car_workshop_franchise
-- Schemas : dim  (wymiary)
--           fact (fakty)
-- =============================================================

-- =============================================================
-- DELETE
-- Usuwa wszystkie wiersze, ale zachowuje historię Delta (time travel działa)
-- =============================================================

-- --- dim -------------------------------------------------------

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
-- Usuwa wszystkie wiersze i czyści historię Delta (szybsze, brak time travel)
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
