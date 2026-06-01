
# ------------------------------------------------------------------
# DIMENSION SCHEMAS
# ------------------------------------------------------------------

# SCD 0
DIM_LOCATIONS_SCHEMA = {
    'location_id':      'BIGINT',
    'location_code':    'STRING',
    'nazwa':            'STRING',
    'typ':              'STRING',
    'ulica':            'STRING',
    'miasto':           'STRING',
    'wojewodztwo':      'STRING',
    'kod_pocztowy':     'STRING',
    'latitude':         'DOUBLE',
    'longitude':        'DOUBLE',
    'telefon':          'STRING',
    'email':            'STRING',
    'kierownik_id':     'BIGINT',
    'liczba_stanowisk': 'BIGINT',
    'powierzchnia_m2':  'BIGINT',
    'data_otwarcia':    'DATE',
    'czy_aktywna':      'BOOLEAN',
}

DIM_EMPLOYEES_SCHEMA = {
    'employee_id':        'BIGINT',
    'employee_code':      'STRING',
    'first_name':         'STRING',
    'last_name':          'STRING',
    'national_id':        'STRING',
    'position':           'STRING',
    'location_id':        'BIGINT',
    'hire_date':          'DATE',
    'termination_date':   'DATE',
    'hourly_rate':        'DOUBLE',
    'is_active':          'BOOLEAN',
}

DIM_CUSTOMERS_SCHEMA = {
    'customer_id':           'BIGINT',
    'customer_code':         'STRING',
    'customer_type':         'STRING',
    'first_name':            'STRING',
    'last_name':             'STRING',
    'company_name':          'STRING',
    'tax_id':                'STRING',
    'email':                 'STRING',
    'phone':                 'STRING',
    'city':                  'STRING',
    'postal_code':           'STRING',
    'registration_date':     'DATE',
    'preferred_location_id': 'BIGINT',
    'marketing_consent':     'BOOLEAN',
}

DIM_VEHICLES_SCHEMA = {
    'vehicle_id':              'BIGINT',
    'customer_id':             'BIGINT',
    'make':                    'STRING',
    'model':                   'STRING',
    'year':                    'BIGINT',
    'vin':                     'STRING',
    'registration_number':     'STRING',
    'fuel_type':               'STRING',
    'engine_displacement':     'DOUBLE',
    'horsepower':              'BIGINT',
    'color':                   'STRING',
    'mileage_km':              'BIGINT',
    'first_registration_date': 'DATE',
}

DIM_PRODUCTS_SCHEMA = {
    'product_id':          'BIGINT',
    'product_code':        'STRING',
    'name':                'STRING',
    'category':            'STRING',
    'manufacturer':        'STRING',
    'purchase_price_net':  'DOUBLE',
    'sale_price_net':      'DOUBLE',
    'vat_rate':            'BIGINT',
    'unit':                'STRING',
    'weight_kg':           'DOUBLE',
    'min_stock_level':     'BIGINT',
    'is_active':           'BOOLEAN',
}

DIM_SERVICES_SCHEMA = {
    'service_id':          'BIGINT',
    'service_code':        'STRING',
    'name':                'STRING',
    'category':            'STRING',
    'min_price_net':       'BIGINT',
    'max_price_net':       'BIGINT',
    'estimated_time_min':  'BIGINT',
    'is_active':           'BOOLEAN',
}

DIM_SUPPLIERS_SCHEMA = {
    'supplier_id':        'BIGINT',
    'supplier_code':      'STRING',
    'name':               'STRING',
    'tax_id':             'STRING',
    'city':               'STRING',
    'address':            'STRING',
    'postal_code':        'STRING',
    'phone':              'STRING',
    'email':              'STRING',
    'contact_person':     'STRING',
    'payment_terms_days': 'BIGINT',
    'min_order_value':    'DOUBLE',
    'is_active':          'BOOLEAN',
}

# ------------------------------------------------------------------
# FACT SCHEMAS
# ------------------------------------------------------------------

FACT_WORK_ORDERS_SCHEMA = {
    'work_order_id':       'BIGINT',
    'work_order_code':     'STRING',
    'location_id':         'BIGINT',
    'customer_id':         'BIGINT',
    'vehicle_id':          'BIGINT',
    'mechanic_id':         'BIGINT',
    'reception_date':      'DATE',
    'completion_date':     'DATE',
    'status':              'STRING',
    'mileage_at_reception':'BIGINT',
    'customer_notes':      'STRING',
    'year':                'BIGINT',
    'month':               'BIGINT',
}

FACT_WORK_ORDER_ITEMS_SCHEMA = {
    'wo_item_id':      'BIGINT',
    'work_order_id':   'BIGINT',
    'item_type':       'STRING',
    'service_id':      'BIGINT',
    'product_id':      'BIGINT',
    'quantity':        'BIGINT',
    'unit_price_net':  'DOUBLE',
    'value_net':       'DOUBLE',
    'vat_rate':        'BIGINT',
    'value_gross':     'DOUBLE',
    'discount_percent':'BIGINT',
}

FACT_SALES_TRANSACTIONS_SCHEMA = {
    'transaction_id':   'BIGINT',
    'transaction_code': 'STRING',
    'location_id':      'BIGINT',
    'customer_id':      'BIGINT',
    'employee_id':      'BIGINT',
    'transaction_date': 'TIMESTAMP',
    'payment_method':   'STRING',
    'receipt_number':   'STRING',
    'year':             'BIGINT',
    'month':            'BIGINT',
}

FACT_SALES_ITEMS_SCHEMA = {
    'sales_item_id':   'BIGINT',
    'transaction_id':  'BIGINT',
    'product_id':      'BIGINT',
    'quantity':        'BIGINT',
    'unit_price_net':  'DOUBLE',
    'discount_percent':'BIGINT',
    'value_net':       'DOUBLE',
    'vat_rate':        'BIGINT',
    'value_gross':     'DOUBLE',
}

FACT_INVOICES_SCHEMA = {
    'invoice_id':       'BIGINT',
    'invoice_code':     'STRING',
    'document_type':    'STRING',
    'source_type':      'STRING',
    'source_id':        'BIGINT',
    'customer_id':      'BIGINT',
    'location_id':      'BIGINT',
    'issue_date':       'DATE',
    'sale_date':        'DATE',
    'payment_due_date': 'DATE',
    'value_net':        'DOUBLE',
    'value_vat':        'DOUBLE',
    'value_gross':      'DOUBLE',
    'status':           'STRING',
    'year':             'BIGINT',
    'month':            'BIGINT',
}

FACT_PAYMENTS_SCHEMA = {
    'payment_id':         'BIGINT',
    'invoice_id':         'BIGINT',
    'payment_date':       'DATE',
    'amount':             'DOUBLE',
    'payment_method':     'STRING',
    'status':             'STRING',
    'transaction_number': 'STRING',
    'year':               'BIGINT',
    'month':              'BIGINT',
}

FACT_INVENTORY_MOVEMENTS_SCHEMA = {
    'movement_id':     'BIGINT',
    'product_id':      'BIGINT',
    'location_id':     'BIGINT',
    'movement_type':   'STRING',
    'quantity':        'BIGINT',
    'movement_date':   'DATE',
    'source_document': 'STRING',
    'document_number': 'STRING',
    'value_net':       'DOUBLE',
    'notes':           'STRING',
    'year':            'BIGINT',
    'month':           'BIGINT',
}

FACT_APPOINTMENTS_SCHEMA = {
    'appointment_id':   'BIGINT',
    'customer_id':      'BIGINT',
    'vehicle_id':       'BIGINT',
    'location_id':      'BIGINT',
    'service_id':       'BIGINT',
    'booking_date':     'DATE',
    'appointment_date': 'TIMESTAMP',
    'status':           'STRING',
    'booking_channel':  'STRING',
    'notes':            'STRING',
    'year':             'BIGINT',
    'month':            'BIGINT',
}

FACT_PURCHASE_ORDERS_SCHEMA = {
    'po_id':                  'BIGINT',
    'po_code':                'STRING',
    'supplier_id':            'BIGINT',
    'location_id':            'BIGINT',
    'order_date':             'DATE',
    'planned_delivery_date':  'DATE',
    'actual_delivery_date':   'DATE',
    'value_net':              'DOUBLE',
    'value_gross':            'DOUBLE',
    'status':                 'STRING',
    'year':                   'BIGINT',
}

FACT_PURCHASE_ORDER_ITEMS_SCHEMA = {
    'po_item_id':         'BIGINT',
    'po_id':              'BIGINT',
    'product_id':         'BIGINT',
    'quantity_ordered':   'BIGINT',
    'quantity_delivered': 'BIGINT',
    'unit_price_net':     'DOUBLE',
    'value_net':          'DOUBLE',
}

FACT_CUSTOMER_FEEDBACK_SCHEMA = {
    'feedback_id':   'BIGINT',
    'customer_id':   'BIGINT',
    'location_id':   'BIGINT',
    'work_order_id': 'BIGINT',
    'feedback_date': 'DATE',
    'rating':        'BIGINT',
    'comment':       'STRING',
    'category':      'STRING',
    'channel':       'STRING',
}

FACT_LOYALTY_PROGRAM_SCHEMA = {
    'loyalty_id':    'BIGINT',
    'customer_id':   'BIGINT',
    'event_date':    'DATE',
    'event_type':    'STRING',
    'points':        'BIGINT',
    'description':   'STRING',
    'balance_after': 'BIGINT',
    'tier':          'STRING',
}

FACT_EMPLOYEE_SCHEDULES_SCHEMA = {
    'schedule_id':    'BIGINT',
    'employee_id':    'BIGINT',
    'date':           'DATE',
    'start_hour':     'BIGINT',
    'end_hour':       'BIGINT',
    'shift_type':     'STRING',
    'overtime_hours': 'BIGINT',
    'attendance':     'STRING',
}
