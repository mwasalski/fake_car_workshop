"""Daily fact generator - logic extracted 1:1 from daily.ipynb.

`generate_day(target_date, scale_factor, output_dir, dims)` writes one day of
parquet for all 13 fact tables under `output_dir/<table>/` (hive-partitioned
per PARTITIONED_TABLES) and returns per-table row stats.

Determinism contract (unchanged from the notebook): RNG is seeded with
TARGET_DATE and every day owns a disjoint ID_BLOCK, so re-running a date
reproduces identical rows (exception: fact_payments.transaction_number uses
uuid4 - a pre-existing leak, tracked in the journal).

`dims` carries the FK pools; build it with `dims_from_frames` (notebook: from
spark.table reads; local lab: from parquet snapshots via `load_dims`).
"""

import os
import random
import uuid
from datetime import date

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from reference_data import *
from table_schemas import PARTITIONED_TABLES

CHUNK_SIZE = 500_000

# rows per day at SCALE_FACTOR = 1.0
DAILY_BASE_ROWS = {
    'fact_work_orders':          2_000,
    'fact_work_order_items':     6_000,
    'fact_sales_transactions':  12_000,
    'fact_sales_items':         36_000,
    'fact_invoices':            10_000,
    'fact_payments':            10_000,
    'fact_inventory_movements': 20_000,
    'fact_appointments':         2_500,
    'fact_purchase_orders':        300,
    'fact_purchase_order_items': 1_200,
    'fact_customer_feedback':      800,
    'fact_loyalty_program':      1_500,
    # fact_employee_schedules: one row per employee per day (not scaled)
}

# every day owns a disjoint ID block per table -> IDs stay unique across days
ID_BLOCK = 10_000_000
EPOCH = date(2020, 1, 1)


def dims_from_frames(locations, employees, customer_ids, vehicle_ids,
                     product_ids, service_ids, supplier_ids):
    """Derive every FK pool the generators need from raw dim data.

    locations: DataFrame[location_id, type]; employees: DataFrame[employee_id,
    position, location_id]; the rest are 1-D numpy arrays of ids.
    """
    for name, ids in [('dim_customers', customer_ids), ('dim_vehicles', vehicle_ids),
                      ('dim_products', product_ids), ('dim_services', service_ids),
                      ('dim_suppliers', supplier_ids),
                      ('dim_locations', locations['location_id'].to_numpy()),
                      ('dim_employees', employees['employee_id'].to_numpy())]:
        if len(ids) == 0:
            raise RuntimeError(f'{name} is empty - generate dimensions first')

    mechanics = employees[employees['position'].isin(POSITIONS['workshop'])]
    sellers = employees[employees['position'].isin(POSITIONS['shop'])]
    return {
        'customer_ids': customer_ids,
        'vehicle_ids': vehicle_ids,
        'product_ids': product_ids,
        'service_ids': service_ids,
        'supplier_ids': supplier_ids,
        'location_ids': locations['location_id'].to_numpy(),
        'workshop_locs': locations.loc[locations['type'] != 'shop', 'location_id'].to_numpy(),
        'shop_locs': locations.loc[locations['type'] != 'workshop', 'location_id'].to_numpy(),
        'employee_ids': employees['employee_id'].to_numpy(),
        'mechanic_ids': mechanics['employee_id'].to_numpy(),
        'seller_ids': sellers['employee_id'].to_numpy(),
        'loc_mechanics': mechanics.groupby('location_id')['employee_id'].apply(np.array).to_dict(),
        'loc_sellers': sellers.groupby('location_id')['employee_id'].apply(np.array).to_dict(),
    }


def generate_day(target_date, scale_factor, output_dir, dims):
    """Generate one day of all 13 fact tables. Returns [{'table', 'rows'}, ...]."""
    TARGET_DATE = target_date
    ROWS = {table: max(int(n * scale_factor), 1) for table, n in DAILY_BASE_ROWS.items()}
    ID_START = (TARGET_DATE - EPOCH).days * ID_BLOCK
    assert max(ROWS.values()) < ID_BLOCK, 'SCALE_FACTOR too large for ID_BLOCK'

    # same TARGET_DATE -> same data
    SEED = int(TARGET_DATE.strftime('%Y%m%d'))
    np.random.seed(SEED)
    random.seed(SEED)

    customer_ids = dims['customer_ids']
    vehicle_ids = dims['vehicle_ids']
    product_ids = dims['product_ids']
    service_ids = dims['service_ids']
    supplier_ids = dims['supplier_ids']
    location_ids = dims['location_ids']
    workshop_locs = dims['workshop_locs']
    shop_locs = dims['shop_locs']
    employee_ids = dims['employee_ids']
    mechanic_ids = dims['mechanic_ids']
    seller_ids = dims['seller_ids']
    loc_mechanics = dims['loc_mechanics']
    loc_sellers = dims['loc_sellers']

    RUN_STATS = []

    # ── helpers ─────────────────────────────────────────────────
    def day_ids(n, offset=0):
        """n consecutive IDs inside this day's ID block."""
        return np.arange(ID_START + offset + 1, ID_START + offset + n + 1)

    def dates_around(low, high, n):
        """Array of dates: TARGET_DATE + random days from [low, high)."""
        offsets = np.random.randint(low, high, n)
        return (pd.Timestamp(TARGET_DATE) + pd.to_timedelta(offsets, 'D')).date

    def same_day(n):
        return np.full(n, TARGET_DATE)

    def pick_staff(loc_arr, staff_by_loc, fallback):
        """Vectorised per-location employee sampling."""
        out = np.empty(len(loc_arr), dtype='int64')
        for loc_id in np.unique(loc_arr):
            pool = staff_by_loc.get(loc_id, fallback)
            mask = loc_arr == loc_id
            out[mask] = np.random.choice(pool, mask.sum())
        return out

    def write_fact(table_name, total_rows, generate_chunk):
        """Generate rows in chunks and write parquet under output_dir/<table>."""
        table_dir = os.path.join(output_dir, table_name)
        os.makedirs(table_dir, exist_ok=True)
        partition_cols = PARTITIONED_TABLES.get(table_name)

        written = 0
        while written < total_rows:
            n = min(CHUNK_SIZE, total_rows - written)
            chunk = pa.Table.from_pandas(generate_chunk(n, written), preserve_index=False)
            if partition_cols:
                pq.write_to_dataset(chunk, root_path=table_dir, partition_cols=partition_cols)
            else:
                fname = f'{table_name}_{TARGET_DATE:%Y%m%d}_{uuid.uuid4().hex[:8]}.parquet'
                pq.write_table(chunk, os.path.join(table_dir, fname))
            written += n

        RUN_STATS.append({'table': table_name, 'rows': written})
        print(f'  {table_name}: {written:,} rows')

    # ── workshop - work orders ──────────────────────────────────
    N_WO = ROWS['fact_work_orders']
    wo_ids = day_ids(N_WO)

    def gen_work_orders(n, offset):
        ids = wo_ids[offset:offset + n]
        loc = np.random.choice(workshop_locs, n)
        return pd.DataFrame({
            'work_order_id': ids,
            'work_order_code': [f'WO-{i:010d}' for i in ids],
            'location_id': loc,
            'customer_id': np.random.choice(customer_ids, n),
            'vehicle_id': np.random.choice(vehicle_ids, n),
            'mechanic_id': pick_staff(loc, loc_mechanics, mechanic_ids),
            'reception_date': same_day(n),
            'completion_date': dates_around(0, 5, n),
            'status': np.random.choice(WORK_ORDER_STATUSES, n, p=STATUS_WEIGHTS),
            'mileage_at_reception': np.random.randint(10_000, 350_000, n),
            'customer_notes': np.random.choice(WORK_ORDER_NOTES, n),
            'year': TARGET_DATE.year,
            'month': TARGET_DATE.month,
        })

    def gen_work_order_items(n, offset):
        item_type = np.random.choice(['service', 'part'], n, p=[0.4, 0.6])
        is_service = item_type == 'service'
        quantity = np.where(is_service, 1, np.random.randint(1, 5, n))
        unit_price = np.round(np.where(is_service,
                                       np.random.uniform(30, 2000, n),
                                       np.random.uniform(5, 500, n)), 2)
        discount = np.random.choice([0, 0, 0, 5, 10, 15], n)
        value_net = np.round(unit_price * quantity * (1 - discount / 100), 2)
        return pd.DataFrame({
            'wo_item_id': day_ids(n, offset),
            'work_order_id': np.random.choice(wo_ids, n),
            'item_type': item_type,
            'service_id': np.where(is_service, np.random.choice(service_ids, n), 0),
            'product_id': np.where(is_service, 0, np.random.choice(product_ids, n)),
            'quantity': quantity,
            'unit_price_net': unit_price,
            'value_net': value_net,
            'vat_rate': 23,
            'value_gross': np.round(value_net * 1.23, 2),
            'discount_percent': discount,
        })

    write_fact('fact_work_orders', N_WO, gen_work_orders)
    write_fact('fact_work_order_items', ROWS['fact_work_order_items'], gen_work_order_items)

    # ── shop - sales ────────────────────────────────────────────
    N_TRX = ROWS['fact_sales_transactions']
    trx_ids = day_ids(N_TRX)

    def gen_sales_transactions(n, offset):
        ids = trx_ids[offset:offset + n]
        loc = np.random.choice(shop_locs, n)
        walk_in = np.random.random(n) < 0.3  # 30% walk-in -> no customer_id
        return pd.DataFrame({
            'transaction_id': ids,
            'transaction_code': [f'TRX-{i:011d}' for i in ids],
            'location_id': loc,
            'customer_id': pd.Series(np.random.choice(customer_ids, n), dtype='Int64').mask(walk_in),
            'employee_id': pick_staff(loc, loc_sellers, seller_ids),
            'transaction_date': same_day(n),
            'payment_method': np.random.choice(PAYMENT_METHODS, n, p=PAYMENT_WEIGHTS),
            'receipt_number': [f'REC/{l:03d}/{i:09d}' for l, i in zip(loc, ids)],
            'year': TARGET_DATE.year,
            'month': TARGET_DATE.month,
        })

    def gen_sales_items(n, offset):
        quantity = np.random.choice([1, 1, 1, 2, 2, 3, 4], n)
        unit_price = np.round(np.random.uniform(3, 600, n), 2)
        discount = np.random.choice([0, 0, 0, 0, 5, 10, 15, 20], n)
        value_net = np.round(unit_price * quantity * (1 - discount / 100), 2)
        return pd.DataFrame({
            'sales_item_id': day_ids(n, offset),
            'transaction_id': np.random.choice(trx_ids, n),
            'product_id': np.random.choice(product_ids, n),
            'quantity': quantity,
            'unit_price_net': unit_price,
            'discount_percent': discount,
            'value_net': value_net,
            'vat_rate': 23,
            'value_gross': np.round(value_net * 1.23, 2),
        })

    write_fact('fact_sales_transactions', N_TRX, gen_sales_transactions)
    write_fact('fact_sales_items', ROWS['fact_sales_items'], gen_sales_items)

    # ── finance - invoices & payments ───────────────────────────
    N_INV = ROWS['fact_invoices']
    invoice_ids = day_ids(N_INV)

    def gen_invoices(n, offset):
        ids = invoice_ids[offset:offset + n]
        source_type = np.random.choice(['work_order', 'sales'], n, p=[0.15, 0.85])
        value_net = np.clip(np.round(np.random.lognormal(4.5, 1.0, n), 2), 10, 50_000)
        value_vat = np.round(value_net * 0.23, 2)
        return pd.DataFrame({
            'invoice_id': ids,
            'invoice_code': [f'INV/{TARGET_DATE.year}/{i:010d}' for i in ids],
            'document_type': np.random.choice(INVOICE_DOCUMENT_TYPES, n, p=INVOICE_DOCUMENT_TYPE_WEIGHTS),
            'source_type': source_type,
            'source_id': np.where(source_type == 'work_order',
                                  np.random.choice(wo_ids, n),
                                  np.random.choice(trx_ids, n)),
            'customer_id': np.random.choice(customer_ids, n),
            'location_id': np.random.choice(location_ids, n),
            'issue_date': same_day(n),
            'sale_date': dates_around(-2, 1, n),
            'payment_due_date': (pd.Timestamp(TARGET_DATE) + pd.to_timedelta(
                np.random.choice([0, 7, 14, 30], n, p=[0.5, 0.15, 0.2, 0.15]), 'D')).date,
            'value_net': value_net,
            'value_vat': value_vat,
            'value_gross': np.round(value_net + value_vat, 2),
            'status': np.random.choice(INVOICE_STATUSES, n, p=INVOICE_STATUS_WEIGHTS),
            'year': TARGET_DATE.year,
            'month': TARGET_DATE.month,
        })

    def gen_payments(n, offset):
        return pd.DataFrame({
            'payment_id': day_ids(n, offset),
            'invoice_id': np.random.choice(invoice_ids, n),
            'payment_date': same_day(n),
            'amount': np.clip(np.round(np.random.lognormal(4.5, 1.0, n), 2), 5, 60_000),
            'payment_method': np.random.choice(PAYMENT_METHODS, n, p=PAYMENT_WEIGHTS),
            'status': np.random.choice(PAYMENT_STATUSES, n, p=PAYMENT_STATUS_WEIGHTS),
            'transaction_number': [f'PAY-{uuid.uuid4().hex[:12].upper()}' for _ in range(n)],
            'year': TARGET_DATE.year,
            'month': TARGET_DATE.month,
        })

    write_fact('fact_invoices', N_INV, gen_invoices)
    write_fact('fact_payments', ROWS['fact_payments'], gen_payments)

    # ── inventory & purchasing ──────────────────────────────────
    N_PO = ROWS['fact_purchase_orders']
    po_ids = day_ids(N_PO)

    def gen_inventory_movements(n, offset):
        ids = day_ids(n, offset)
        movement_type = np.random.choice(MOVEMENT_TYPES, n, p=MOVEMENT_TYPE_WEIGHTS)
        quantity = np.random.randint(1, 20, n)
        quantity = np.where(np.isin(movement_type, ['issue_sales', 'issue_workshop']), -quantity, quantity)
        return pd.DataFrame({
            'movement_id': ids,
            'product_id': np.random.choice(product_ids, n),
            'location_id': np.random.choice(location_ids, n),
            'movement_type': movement_type,
            'quantity': quantity,
            'movement_date': same_day(n),
            'source_document': np.random.choice(SOURCE_DOCUMENTS, n),
            'document_number': [f'DOC-{i:011d}' for i in ids],
            'value_net': np.round(np.abs(quantity) * np.random.uniform(5, 500, n), 2),
            'notes': np.random.choice(INVENTORY_NOTES, n),
            'year': TARGET_DATE.year,
            'month': TARGET_DATE.month,
        })

    def gen_purchase_orders(n, offset):
        ids = po_ids[offset:offset + n]
        value_net = np.clip(np.round(np.random.lognormal(7, 0.8, n), 2), 200, 100_000)
        return pd.DataFrame({
            'po_id': ids,
            'po_code': [f'PO-{i:09d}' for i in ids],
            'supplier_id': np.random.choice(supplier_ids, n),
            'location_id': np.random.choice(location_ids, n),
            'order_date': same_day(n),
            'planned_delivery_date': dates_around(3, 21, n),
            'actual_delivery_date': dates_around(3, 25, n),
            'value_net': value_net,
            'value_gross': np.round(value_net * 1.23, 2),
            'status': np.random.choice(PO_STATUSES, n, p=PO_STATUS_WEIGHTS),
            'year': TARGET_DATE.year,
        })

    def gen_purchase_order_items(n, offset):
        quantity = np.random.randint(1, 50, n)
        unit_price = np.round(np.random.uniform(5, 500, n), 2)
        return pd.DataFrame({
            'po_item_id': day_ids(n, offset),
            'po_id': np.random.choice(po_ids, n),
            'product_id': np.random.choice(product_ids, n),
            'quantity_ordered': quantity,
            'quantity_delivered': np.clip(quantity + np.random.randint(-2, 1, n), 0, None),
            'unit_price_net': unit_price,
            'value_net': np.round(unit_price * quantity, 2),
        })

    write_fact('fact_inventory_movements', ROWS['fact_inventory_movements'], gen_inventory_movements)
    write_fact('fact_purchase_orders', N_PO, gen_purchase_orders)
    write_fact('fact_purchase_order_items', ROWS['fact_purchase_order_items'], gen_purchase_order_items)

    # ── customers & staff ───────────────────────────────────────
    def gen_appointments(n, offset):
        return pd.DataFrame({
            'appointment_id': day_ids(n, offset),
            'customer_id': np.random.choice(customer_ids, n),
            'vehicle_id': np.random.choice(vehicle_ids, n),
            'location_id': np.random.choice(workshop_locs, n),
            'service_id': np.random.choice(service_ids, n),
            'booking_date': dates_around(-14, 0, n),
            'appointment_date': same_day(n),
            'status': np.random.choice(APPOINTMENT_STATUSES, n, p=APPOINTMENT_STATUS_WEIGHTS),
            'booking_channel': np.random.choice(BOOKING_CHANNELS, n, p=BOOKING_CHANNEL_WEIGHTS),
            'notes': np.random.choice(APPOINTMENT_NOTES, n),
            'year': TARGET_DATE.year,
            'month': TARGET_DATE.month,
        })

    def gen_customer_feedback(n, offset):
        return pd.DataFrame({
            'feedback_id': day_ids(n, offset),
            'customer_id': np.random.choice(customer_ids, n),
            'location_id': np.random.choice(location_ids, n),
            'work_order_id': np.random.choice(wo_ids, n),
            'feedback_date': same_day(n),
            'rating': np.random.choice([1, 2, 3, 4, 5], n, p=RATING_WEIGHTS),
            'comment': np.random.choice(FEEDBACK_COMMENTS, n),
            'category': np.random.choice(FEEDBACK_CATEGORIES, n, p=FEEDBACK_CATEGORY_WEIGHTS),
            'channel': np.random.choice(FEEDBACK_CHANNELS, n, p=FEEDBACK_CHANNEL_WEIGHTS),
        })

    def gen_loyalty_program(n, offset):
        return pd.DataFrame({
            'loyalty_id': day_ids(n, offset),
            'customer_id': np.random.choice(customer_ids, n),
            'event_date': same_day(n),
            'event_type': np.random.choice(LOYALTY_EVENT_TYPES, n, p=LOYALTY_EVENT_WEIGHTS),
            'points': np.random.choice(LOYALTY_POINTS, n, p=LOYALTY_POINT_WEIGHTS),
            'description': np.random.choice(LOYALTY_DESCRIPTIONS, n),
            'balance_after': np.random.randint(0, 5000, n),
            'tier': np.random.choice(LOYALTY_TIERS, n, p=LOYALTY_TIER_WEIGHTS),
        })

    def gen_employee_schedules(n, offset):
        start_hour = np.random.choice(SHIFT_START_HOURS, n, p=SHIFT_START_WEIGHTS)
        length = np.random.choice(SHIFT_LENGTHS, n, p=SHIFT_LENGTH_WEIGHTS)
        return pd.DataFrame({
            'schedule_id': day_ids(n, offset),
            'employee_id': employee_ids[offset:offset + n],
            'date': same_day(n),
            'start_hour': start_hour,
            'end_hour': start_hour + length,
            'shift_type': np.random.choice(SHIFT_TYPES, n, p=SHIFT_TYPE_WEIGHTS),
            'overtime_hours': np.random.choice([0, 0, 0, 0, 0, 1, 2, 3, 4], n),
            'attendance': np.random.choice(ATTENDANCE_TYPES, n, p=ATTENDANCE_WEIGHTS),
        })

    write_fact('fact_appointments', ROWS['fact_appointments'], gen_appointments)
    write_fact('fact_customer_feedback', ROWS['fact_customer_feedback'], gen_customer_feedback)
    write_fact('fact_loyalty_program', ROWS['fact_loyalty_program'], gen_loyalty_program)
    write_fact('fact_employee_schedules', len(employee_ids), gen_employee_schedules)  # 1 row per employee

    return RUN_STATS
