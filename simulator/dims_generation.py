"""Dimension generator - logic extracted 1:1 from initial_dims.ipynb.

`generate_dims(save_fn)` builds the 7 dimension tables (seed 42) and hands each
pandas DataFrame to `save_fn(pdf, table_name)` - the caller decides where it
goes (Spark -> parquet on the Databricks volume, pyarrow -> local disk, ...).

`scale` shrinks the three big tables (customers/vehicles/suppliers) for fast
local runs. scale=1.0 reproduces the exact Databricks dataset; anything else
is for local testing only - RNG consumption changes, so rows differ.
"""

import random
from datetime import date

import numpy as np
import pandas as pd
from faker import Faker

from reference_data import *
from table_schemas import TABLE_SCHEMAS  # noqa: F401  (re-export convenience)

HIST_START = date(2020, 1, 1)  # customer/vehicle history starts here


def generate_dims(save_fn, scale=1.0):
    """Build all 7 dims and pass each to save_fn(pdf, table_name). Returns stats."""
    fake = Faker('pl_PL')  # Polish names/addresses fit the scenario; column names stay English
    Faker.seed(42)
    np.random.seed(42)
    random.seed(42)

    NUM_LOCATIONS = len(CITIES)  # one location per city
    NUM_CUSTOMERS = max(int(50_000 * scale), 50)
    NUM_VEHICLES = max(int(65_000 * scale), 50)
    NUM_SUPPLIERS = max(int(500 * scale), 10)
    TODAY = date.today()

    def random_dates(start, end, n):
        """Array of python dates uniformly drawn from [start, end]."""
        days = np.random.randint(0, (end - start).days + 1, n)
        return (pd.Timestamp(start) + pd.to_timedelta(days, 'D')).date

    stats = []

    def save(pdf, table_name):
        save_fn(pdf, table_name)
        stats.append({'table': table_name, 'rows': len(pdf)})

    # ── locations & employees ───────────────────────────────────
    locations = []
    for i, (city, region, lat, lon) in enumerate(CITIES[:NUM_LOCATIONS]):
        loc_type = np.random.choice(LOCATION_TYPES, p=LOCATION_TYPE_WEIGHTS)
        locations.append({
            'location_id': i + 1,
            'location_code': f'LOC-{i + 1:03d}',
            'name': f'AutoService {city}',
            'type': loc_type,
            'street': fake.street_address(),
            'city': city,
            'region': region,
            'postal_code': fake.postcode(),
            'latitude': lat + np.random.uniform(-0.02, 0.02),
            'longitude': lon + np.random.uniform(-0.02, 0.02),
            'phone': fake.phone_number(),
            'email': f'service.{city.lower().replace(" ", "").replace("-", "")}@autoservice.pl',
            'manager_id': None,  # backfilled below once employees exist
            'number_of_bays': int(np.random.randint(4, 12)) if loc_type != 'shop' else 0,
            'area_m2': int(np.random.randint(200, 800)),
            'opening_date': fake.date_between(date(2005, 1, 1), date(2020, 6, 30)),
            'is_active': i < NUM_LOCATIONS - 5,  # last 5 locations are closed
        })
    df_locations = pd.DataFrame(locations)

    employees = []
    for loc in locations:
        # (position, hourly rate range, probability the employee has quit)
        staff = [(position, 45, 80, 0.00) for position in POSITIONS['management']]
        if loc['type'] != 'shop':      # workshop or workshop_and_shop
            staff += [(random.choice(POSITIONS['workshop']), 30, 65, 0.10)
                      for _ in range(np.random.randint(5, 10))]
        if loc['type'] != 'workshop':  # shop or workshop_and_shop
            staff += [(random.choice(POSITIONS['shop']), 25, 45, 0.15)
                      for _ in range(np.random.randint(3, 7))]

        for position, rate_lo, rate_hi, quit_prob in staff:
            emp_id = len(employees) + 1
            hire_date = fake.date_between(loc['opening_date'], TODAY)
            has_quit = random.random() < quit_prob
            employees.append({
                'employee_id': emp_id,
                'employee_code': f'EMP-{emp_id:05d}',
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'national_id': fake.pesel(),
                'position': position,
                'location_id': loc['location_id'],
                'hire_date': hire_date,
                'termination_date': fake.date_between(hire_date, TODAY) if has_quit else None,
                'hourly_rate': round(np.random.uniform(rate_lo, rate_hi), 2),
                'is_active': loc['is_active'] and not has_quit,
            })
    df_employees = pd.DataFrame(employees)

    # managers exist now -> backfill dim_locations.manager_id
    managers = (df_employees[df_employees['position'] == 'branch_manager']
                .groupby('location_id')['employee_id'].first())
    df_locations['manager_id'] = df_locations['location_id'].map(managers).astype('Int64')

    save(df_locations, 'dim_locations')
    save(df_employees, 'dim_employees')

    # ── customers ───────────────────────────────────────────────
    customer_type = np.random.choice(['individual', 'business'], NUM_CUSTOMERS, p=[0.7, 0.3])
    is_individual = customer_type == 'individual'

    df_customers = pd.DataFrame({
        'customer_id': np.arange(1, NUM_CUSTOMERS + 1),
        'customer_code': [f'CUS-{i:07d}' for i in range(1, NUM_CUSTOMERS + 1)],
        'customer_type': customer_type,
        'first_name': [fake.first_name() if ind else '' for ind in is_individual],
        'last_name': [fake.last_name() if ind else '' for ind in is_individual],
        'company_name': ['' if ind else fake.company() for ind in is_individual],
        'tax_id': ['' if ind else fake.company_vat() for ind in is_individual],
        'email': [fake.email() for _ in range(NUM_CUSTOMERS)],
        'phone': [fake.phone_number() for _ in range(NUM_CUSTOMERS)],
        'city': np.random.choice([c[0] for c in CITIES], NUM_CUSTOMERS),
        'postal_code': [fake.postcode() for _ in range(NUM_CUSTOMERS)],
        'registration_date': random_dates(HIST_START, TODAY, NUM_CUSTOMERS),
        'preferred_location_id': np.random.choice(df_locations['location_id'].to_numpy(), NUM_CUSTOMERS),
        'marketing_consent': np.random.choice([True, False], NUM_CUSTOMERS, p=[0.6, 0.4]),
    })
    save(df_customers, 'dim_customers')

    # ── vehicles ────────────────────────────────────────────────
    makes = list(CAR_MAKES)
    make_probs = np.array([MAKE_WEIGHTS[m] for m in makes])
    make_probs = make_probs / make_probs.sum()
    chosen_makes = np.random.choice(makes, NUM_VEHICLES, p=make_probs)

    df_vehicles = pd.DataFrame({
        'vehicle_id': np.arange(1, NUM_VEHICLES + 1),
        'customer_id': np.random.choice(df_customers['customer_id'].to_numpy(), NUM_VEHICLES),
        'make': chosen_makes,
        'model': [random.choice(CAR_MAKES[m]) for m in chosen_makes],
        'year': np.random.randint(2005, TODAY.year + 1, NUM_VEHICLES),
        'vin': [fake.bothify('???##############').upper() for _ in range(NUM_VEHICLES)],  # 17 chars
        'registration_number': [fake.license_plate() for _ in range(NUM_VEHICLES)],
        'fuel_type': np.random.choice(FUEL_TYPES, NUM_VEHICLES, p=FUEL_WEIGHTS),
        'engine_displacement': np.random.choice(
            [1.0, 1.2, 1.4, 1.5, 1.6, 1.8, 2.0, 2.2, 2.5, 3.0], NUM_VEHICLES,
            p=[0.05, 0.10, 0.15, 0.12, 0.18, 0.12, 0.12, 0.06, 0.05, 0.05]),
        'horsepower': np.random.randint(60, 350, NUM_VEHICLES),
        'color': np.random.choice(COLORS, NUM_VEHICLES),
        'mileage_km': np.random.randint(5_000, 350_000, NUM_VEHICLES),
        'first_registration_date': random_dates(date(2005, 1, 1), TODAY, NUM_VEHICLES),
    })
    save(df_vehicles, 'dim_vehicles')

    # ── products, services & suppliers ──────────────────────────
    def base_price(name):
        lower = name.lower()
        for keyword, lo, hi in PRODUCT_PRICE_RULES:
            if keyword in lower:
                return round(np.random.uniform(lo, hi), 2)
        return round(np.random.uniform(*DEFAULT_PRICE_RANGE), 2)

    products = []
    for category, names in PRODUCT_CATEGORIES.items():
        for name in names:
            for manufacturer in random.sample(MANUFACTURERS, random.randint(2, 6)):
                price = base_price(name)
                products.append({
                    'product_id': len(products) + 1,
                    'product_code': f'PRD-{len(products) + 1:06d}',
                    'name': f'{name} {manufacturer}',
                    'category': category,
                    'manufacturer': manufacturer,
                    'purchase_price_net': price,
                    'sale_price_net': round(price * np.random.uniform(1.15, 1.45), 2),
                    'vat_rate': 23,
                    'unit': 'L' if category == 'Oils and Fluids' else 'pcs',
                    'weight_kg': round(np.random.uniform(0.1, 15), 2),
                    'min_stock_level': int(np.random.randint(2, 20)),
                    'is_active': random.random() > 0.05,
                })
    save(pd.DataFrame(products), 'dim_products')

    df_services = pd.DataFrame([{
        'service_id': i + 1,
        'service_code': f'SRV-{i + 1:03d}',
        'name': name,
        'category': category,
        'min_price_net': price_lo,
        'max_price_net': price_hi,
        'estimated_time_min': minutes,
        'is_active': True,
    } for i, (name, category, price_lo, price_hi, minutes) in enumerate(SERVICE_CATALOGUE)])
    save(df_services, 'dim_services')

    df_suppliers = pd.DataFrame([{
        'supplier_id': i + 1,
        'supplier_code': f'SUP-{i + 1:04d}',
        'name': fake.company(),
        'tax_id': fake.company_vat(),
        'city': random.choice([c[0] for c in CITIES]),
        'address': fake.street_address(),
        'postal_code': fake.postcode(),
        'phone': fake.phone_number(),
        'email': fake.company_email(),
        'contact_person': fake.name(),
        'payment_terms_days': random.choice([14, 21, 30, 45, 60]),
        'min_order_value': round(np.random.uniform(200, 2000), 2),
        'is_active': random.random() > 0.08,
    } for i in range(NUM_SUPPLIERS)])
    save(df_suppliers, 'dim_suppliers')

    return stats
