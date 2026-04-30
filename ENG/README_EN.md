# Data Generator – Car Workshop & Accessories Shop Network

## Project Description

A generator of realistic business data simulating the operations of a network of **100 car workshops and accessories shops** located across cities throughout Poland. The data covers a **5-year period (2020–2024)** and can reach **~30 GB** at full scale.

The project was created for learning how to work with large datasets in **Databricks** (partitioning, Delta Lake, Z-ordering, query optimisation).

---

## Requirements

```bash
pip install pandas pyarrow faker tqdm numpy
```

Python 3.9+

---

## Quick Start

1. Open the notebook `warsztat_generator.ipynb`
2. In the **CONFIGURATION** cell, set the parameters:

```python
SCALE_FACTOR = 0.01    # 0.01 = ~300 MB, 0.1 = ~3 GB, 1.0 = ~30 GB
OUTPUT_DIR = './output_data'
OUTPUT_FORMAT = 'parquet'  # or 'csv'
```

3. Run all cells (Run All)
4. Data will appear in the `./output_data/` directory

---

## Scale Configuration

| SCALE_FACTOR | Data Size | Generation Time* | Use Case |
|:---:|:---:|:---:|---|
| `0.01` | ~300 MB | ~2–5 min | Quick tests, prototyping |
| `0.1` | ~3 GB | ~20–40 min | Learning partitioning |
| `0.5` | ~15 GB | ~2–3 h | Performance testing |
| `1.0` | ~30 GB | ~4–6 h | Full production dataset |

*\*Approximate time on a machine with 16 GB RAM*

---

## Data Model

### Relationship Schema

```
dim_suppliers ──┐
                ├── fact_purchase_orders ── fact_purchase_order_items
                │
dim_locations ──┼── dim_employees ── fact_employee_schedules
                │
dim_customers ──┼── dim_vehicles
    │           │
    │           ├── fact_work_orders ── fact_work_order_items ──┐
    │           │       │                                       │
    │           │       ├── fact_appointments                   ├── dim_products
    │           │       └── fact_customer_feedback               │
    │           │                                               │
    │           ├── fact_sales_transactions ── fact_sales_items ─┘
    │           │
    │           ├── fact_invoices ── fact_payments
    │           │
    │           ├── fact_inventory_movements
    │           │
    └───────────┴── fact_loyalty_program
                                                        dim_services ──── fact_work_order_items
```

### Dimension Tables

| Table | Rows | Description |
|---|:---:|---|
| `dim_locations` | 100 | Workshop/store locations – city, type (workshop/store/both), address, GPS coordinates |
| `dim_employees` | ~2,000 | Employees – mechanics, sales staff, managers, diagnosticians; assigned to locations |
| `dim_customers` | 500K | Individual customers (70%) and business customers (30%) with tax ID |
| `dim_vehicles` | 600K | Customer vehicles – 20 makes, models, VIN, fuel type, mileage |
| `dim_products` | ~15,000 | Products across 15 categories (oils, filters, tyres, batteries, chemicals, accessories…) with manufacturer variants |
| `dim_services` | 48 | Workshop service catalogue with min/max prices and estimated duration |
| `dim_suppliers` | 300 | Parts suppliers with payment terms |

### Fact Tables

| Table | Rows (SCALE=1.0) | Description |
|---|:---:|---|
| `fact_work_orders` | 5M | Workshop work orders – customer, vehicle, mechanic, status, notes |
| `fact_work_order_items` | 15M | Work order line items – services (40%) and parts (60%) with prices and VAT |
| `fact_sales_transactions` | 30M | Retail sales transactions – receipt, payment method |
| `fact_sales_items` | 90M | Sales line items – product, quantity, price, discount |
| `fact_invoices` | 35M | VAT invoices, receipts, corrections – linked to work orders and sales |
| `fact_payments` | 35M | Payments – cash, card, bank transfer, BLIK, leasing |
| `fact_inventory_movements` | 50M | Inventory movements – goods receipts (GR), goods issues (GI), returns, stock counts |

### Supporting Tables

| Table | Rows (SCALE=1.0) | Description |
|---|:---:|---|
| `fact_appointments` | 5M | Appointment bookings – channel (phone/online/in-person), status |
| `fact_purchase_orders` | 500K | Supplier purchase orders with planned and actual delivery dates |
| `fact_purchase_order_items` | 2M | Purchase order line items – quantity ordered vs delivered |
| `fact_customer_feedback` | 2M | Customer feedback – rating 1–5, comments, category |
| `fact_loyalty_program` | 500K | Loyalty programme – points, tiers (standard/silver/gold/platinum) |
| `fact_employee_schedules` | 3M | Work schedules – shifts, overtime, attendance |

---

## Keys and Relationships (FK)

```
fact_work_orders.location_id       → dim_locations.location_id
fact_work_orders.customer_id       → dim_customers.customer_id
fact_work_orders.vehicle_id        → dim_vehicles.vehicle_id
fact_work_orders.mechanic_id       → dim_employees.employee_id

fact_work_order_items.work_order_id → fact_work_orders.work_order_id
fact_work_order_items.service_id    → dim_services.service_id
fact_work_order_items.product_id    → dim_products.product_id

fact_sales_transactions.location_id → dim_locations.location_id
fact_sales_transactions.customer_id → dim_customers.customer_id
fact_sales_transactions.employee_id → dim_employees.employee_id

fact_sales_items.transaction_id     → fact_sales_transactions.transaction_id
fact_sales_items.product_id         → dim_products.product_id

fact_invoices.customer_id           → dim_customers.customer_id
fact_invoices.location_id           → dim_locations.location_id

fact_payments.invoice_id            → fact_invoices.invoice_id

fact_inventory_movements.product_id  → dim_products.product_id
fact_inventory_movements.location_id → dim_locations.location_id

fact_appointments.customer_id       → dim_customers.customer_id
fact_appointments.vehicle_id        → dim_vehicles.vehicle_id
fact_appointments.location_id       → dim_locations.location_id
fact_appointments.service_id        → dim_services.service_id

fact_purchase_orders.supplier_id    → dim_suppliers.supplier_id
fact_purchase_orders.location_id    → dim_locations.location_id
fact_purchase_order_items.po_id     → fact_purchase_orders.po_id
fact_purchase_order_items.product_id → dim_products.product_id

fact_customer_feedback.customer_id  → dim_customers.customer_id
fact_customer_feedback.location_id  → dim_locations.location_id
fact_customer_feedback.work_order_id → fact_work_orders.work_order_id

fact_loyalty_program.customer_id    → dim_customers.customer_id
fact_employee_schedules.employee_id → dim_employees.employee_id

dim_vehicles.customer_id            → dim_customers.customer_id
dim_employees.location_id           → dim_locations.location_id
```

---

## Built-in Data Realism

- **Seasonality** – more work orders in March/April (summer tyre changeover) and October/November (winter tyres)
- **Price distribution** – log-normal (most transactions are low-value, fewer are high-value)
- **Brand popularity** – Toyota 12%, VW 11%, Skoda 10%… aligned with the Polish market
- **Fuel types** – petrol 35%, diesel 30%, LPG 15%, hybrid 15%, electric 5%
- **Payment methods** – card 40%, bank transfer 20%, cash 15%, BLIK 15%
- **Working hours** – transaction distribution mirrors opening hours (7–19)
- **Polish data** – first names, surnames, PESEL, tax IDs, addresses, cities (Faker pl_PL)

---

## Output File Structure

```
output_data/
├── dim_locations/
│   └── dim_locations.parquet
├── dim_employees/
│   └── dim_employees.parquet
├── dim_customers/
│   └── dim_customers.parquet
├── dim_vehicles/
│   └── dim_vehicles.parquet
├── dim_products/
│   └── dim_products.parquet
├── dim_services/
│   └── dim_services.parquet
├── dim_suppliers/
│   └── dim_suppliers.parquet
├── fact_work_orders/              ← partitioned
│   ├── year=2020/month=1/
│   ├── year=2020/month=2/
│   └── ...
├── fact_work_order_items/
├── fact_sales_transactions/       ← partitioned
│   ├── year=2020/month=1/
│   └── ...
├── fact_sales_items/
├── fact_invoices/                 ← partitioned
├── fact_payments/                 ← partitioned
├── fact_inventory_movements/      ← partitioned
├── fact_appointments/             ← partitioned
├── fact_purchase_orders/
├── fact_purchase_order_items/
├── fact_customer_feedback/
├── fact_loyalty_program/
└── fact_employee_schedules/
```

Fact tables with dates are partitioned by `year/month` – ideal for learning partition pruning in Databricks.

---

## Loading into Databricks

### Option 1: DBFS Upload + Spark

```python
# After uploading to DBFS
df_locations = spark.read.parquet("dbfs:/FileStore/output_data/dim_locations/")
df_work_orders = spark.read.parquet("dbfs:/FileStore/output_data/fact_work_orders/")

# Partitioned tables automatically detect the year/month columns
df_work_orders.printSchema()
```

### Option 2: Unity Catalog Volume

```python
df = spark.read.parquet("/Volumes/catalog/schema/volume/output_data/dim_customers/")
```

### Option 3: Managed Table with Delta

```sql
CREATE TABLE car_workshop.dim_locations
USING DELTA
AS SELECT * FROM parquet.`dbfs:/FileStore/output_data/dim_locations/`;

CREATE TABLE car_workshop.fact_work_orders
USING DELTA
PARTITIONED BY (year, month)
AS SELECT * FROM parquet.`dbfs:/FileStore/output_data/fact_work_orders/`;

-- Optimisation
OPTIMIZE car_workshop.fact_work_orders ZORDER BY (location_id, customer_id);
```

---

## Sample Analytical Queries

```sql
-- Top 10 locations by revenue
SELECT l.city, COUNT(*) AS work_orders, SUM(i.value_gross) AS revenue
FROM fact_work_orders w
JOIN fact_work_order_items i ON w.work_order_id = i.work_order_id
JOIN dim_locations l ON w.location_id = l.location_id
GROUP BY l.city
ORDER BY revenue DESC
LIMIT 10;

-- Seasonality of work orders
SELECT year, month, COUNT(*) AS num_work_orders
FROM fact_work_orders
GROUP BY year, month
ORDER BY year, month;

-- Most popular services
SELECT s.name, COUNT(*) AS count
FROM fact_work_order_items wi
JOIN dim_services s ON wi.service_id = s.service_id
WHERE wi.item_type = 'service'
GROUP BY s.name
ORDER BY count DESC;

-- Average rating per location
SELECT l.city, ROUND(AVG(f.rating), 2) AS avg_rating, COUNT(*) AS n
FROM fact_customer_feedback f
JOIN dim_locations l ON f.location_id = l.location_id
GROUP BY l.city
ORDER BY avg_rating DESC;
```
