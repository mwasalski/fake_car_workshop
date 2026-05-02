# Databricks notebook source

# MAGIC %md
# MAGIC # Data Generator – Car Workshop & Accessories Shop Network
# MAGIC
# MAGIC **Business scenario:** Network of 100 car workshops and accessories shops across Poland.
# MAGIC **Period:** configurable via DATE_START / DATE_END
# MAGIC **Scale:** ~10-50 GB (configurable via SCALE_FACTOR)
# MAGIC
# MAGIC ## Tables:
# MAGIC ### Dimension
# MAGIC 1. `dim_locations` - locations (100)
# MAGIC 2. `dim_employees` - employees (~2000)
# MAGIC 3. `dim_customers` - customers (~500K)
# MAGIC 4. `dim_vehicles` - vehicles (~600K)
# MAGIC 5. `dim_products` - products/parts (~15K)
# MAGIC 6. `dim_services` - service catalogue
# MAGIC 7. `dim_suppliers` - suppliers (~300)
# MAGIC ### Fact
# MAGIC 8. `fact_work_orders`
# MAGIC 9. `fact_work_order_items`
# MAGIC 10. `fact_sales_transactions`
# MAGIC 11. `fact_sales_items`
# MAGIC 12. `fact_invoices`
# MAGIC 13. `fact_payments`
# MAGIC 14. `fact_inventory_movements`
# MAGIC 15. `fact_appointments`
# MAGIC 16. `fact_purchase_orders`
# MAGIC 17. `fact_purchase_order_items`
# MAGIC 18. `fact_customer_feedback`
# MAGIC 19. `fact_loyalty_program`
# MAGIC 20. `fact_employee_schedules`

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import timedelta, date
from faker import Faker
import random
import uuid

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    BooleanType, DateType, TimestampType,
)

fake = Faker('pl_PL')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

print('Libraries loaded OK')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets – Single-day mode for Auto Loader testing
# MAGIC
# MAGIC - `SINGLE_DAY_MODE = True`  → generate data only for TARGET_DATE (Auto Loader delta test)
# MAGIC - `SINGLE_DAY_MODE = False` → full historical run

# COMMAND ----------

dbutils.widgets.dropdown(
    'SINGLE_DAY_MODE', 'False', ['False', 'True'],
    label='Single Day Mode  (Auto Loader test)'
)
dbutils.widgets.text(
    'TARGET_DATE', str(date.today()),
    label='Target Date  (YYYY-MM-DD, used when Single Day Mode = True)'
)
_SINGLE_DAY_MODE = dbutils.widgets.get('SINGLE_DAY_MODE') == 'True'
_TARGET_DATE_STR = dbutils.widgets.get('TARGET_DATE')

print(f'SINGLE_DAY_MODE = {_SINGLE_DAY_MODE}')
if _SINGLE_DAY_MODE:
    print(f'TARGET_DATE     = {_TARGET_DATE_STR}')

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

# SCALE_FACTOR: 1.0 = full data (~30 GB), 0.1 = ~3 GB, 0.01 = ~300 MB for testing
SCALE_FACTOR = 1.0

OUTPUT_DIR_DIM  = '/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files'
OUTPUT_DIR_FACT = '/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files'

DATE_START = date(2025, 1, 1)
DATE_END   = date(2026, 5, 1)

if _SINGLE_DAY_MODE:
    DATE_START = date.fromisoformat(_TARGET_DATE_STR)
    DATE_END   = date.fromisoformat(_TARGET_DATE_STR)
    print(f'SINGLE_DAY_MODE active → generating data for {DATE_START} only')

NUM_YEARS     = max(DATE_END.year - DATE_START.year + 1, 1)
CHUNK_SIZE    = 200_000
NUM_LOCATIONS = 100

print(f'SCALE_FACTOR    = {SCALE_FACTOR}')
print(f'OUTPUT_DIR_DIM  = {OUTPUT_DIR_DIM}')
print(f'OUTPUT_DIR_FACT = {OUTPUT_DIR_FACT}')
print(f'DATE_START      = {DATE_START}')
print(f'DATE_END        = {DATE_END}')
print(f'NUM_YEARS       = {NUM_YEARS}')
print(f'Estimated data size: ~{SCALE_FACTOR * NUM_YEARS / 7 * 30:.1f} GB')

# COMMAND ----------

# ============================================================
# HELPERS
# ============================================================

def _get_base_dir(table_name):
    return OUTPUT_DIR_DIM if table_name.startswith('dim_') else OUTPUT_DIR_FACT


def save_table(df_pandas, table_name, partition_cols=None):
    """Converts a pandas DataFrame to a Spark DataFrame and writes it as parquet."""
    table_dir = f"{_get_base_dir(table_name)}/{table_name}"
    sdf = spark.createDataFrame(df_pandas)
    writer = sdf.write.mode('overwrite')
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(table_dir)
    print(f'  ✓ {table_name}: {len(df_pandas):,} rows')


def save_table_chunked(generate_func, table_name, total_rows, partition_cols=None):
    """Generates data in pandas chunks and writes each chunk via Spark."""
    table_dir = f"{_get_base_dir(table_name)}/{table_name}"
    rows_written = 0
    chunk_num = 0
    while rows_written < total_rows:
        chunk_rows = min(CHUNK_SIZE, total_rows - rows_written)
        df_chunk = generate_func(chunk_rows, rows_written)
        sdf = spark.createDataFrame(df_chunk)
        # overwrite on first chunk clears any previous run; subsequent chunks append
        mode = 'overwrite' if chunk_num == 0 else 'append'
        writer = sdf.write.mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(table_dir)
        rows_written += chunk_rows
        chunk_num += 1
        if chunk_num % 5 == 0 or rows_written >= total_rows:
            print(f'  {table_name}: {rows_written:,}/{total_rows:,} rows')
    print(f'  ✓ {table_name}: {rows_written:,} rows in {chunk_num} chunks')


def random_dates(start, end, n):
    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end)
    delta    = max((end_ts - start_ts).days, 1)
    return start_ts + pd.to_timedelta(np.random.randint(0, delta, size=n), unit='D')


def seasonal_dates(start, end, n):
    """Random dates biased towards spring/autumn (tyre-change seasons)."""
    dates = random_dates(start, end, n)
    months = dates.month
    seasonal_weights = {
        1: 0.7, 2: 0.7, 3: 1.4, 4: 1.4, 5: 1.0, 6: 0.9,
        7: 0.8, 8: 0.8, 9: 1.0, 10: 1.4, 11: 1.3, 12: 0.6,
    }
    weights = np.array([seasonal_weights[m] for m in months])
    weights = weights / weights.sum()
    indices = np.random.choice(len(dates), size=n, replace=True, p=weights)
    return dates[indices]


print('Helpers loaded OK')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reference Data (Dictionaries)

# COMMAND ----------

# ============================================================
# REFERENCE DATA DICTIONARIES
# ============================================================

REGIONS = [
    'Lower Silesian', 'Kuyavian-Pomeranian', 'Lublin', 'Lubusz',
    'Lodz', 'Lesser Poland', 'Masovian', 'Opole',
    'Subcarpathian', 'Podlaskie', 'Pomeranian', 'Silesian',
    'Holy Cross', 'Warmian-Masurian', 'Greater Poland', 'West Pomeranian'
]

CITIES = [
    ('Warsaw',               'Masovian',              52.2297, 21.0122),
    ('Krakow',               'Lesser Poland',         50.0647, 19.9450),
    ('Lodz',                 'Lodz',                  51.7592, 19.4560),
    ('Wroclaw',              'Lower Silesian',        51.1079, 17.0385),
    ('Poznan',               'Greater Poland',        52.4064, 16.9252),
    ('Gdansk',               'Pomeranian',            54.3520, 18.6466),
    ('Szczecin',             'West Pomeranian',       53.4285, 14.5528),
    ('Bydgoszcz',            'Kuyavian-Pomeranian',   53.1235, 18.0084),
    ('Lublin',               'Lublin',                51.2465, 22.5684),
    ('Bialystok',            'Podlaskie',             53.1325, 23.1688),
    ('Katowice',             'Silesian',              50.2649, 19.0238),
    ('Gdynia',               'Pomeranian',            54.5189, 18.5305),
    ('Czestochowa',          'Silesian',              50.8118, 19.1203),
    ('Radom',                'Masovian',              51.4027, 21.1471),
    ('Sosnowiec',            'Silesian',              50.2863, 19.1041),
    ('Torun',                'Kuyavian-Pomeranian',   53.0138, 18.5984),
    ('Kielce',               'Holy Cross',            50.8661, 20.6286),
    ('Rzeszow',              'Subcarpathian',         50.0412, 21.9991),
    ('Gliwice',              'Silesian',              50.2945, 18.6714),
    ('Zabrze',               'Silesian',              50.3249, 18.7857),
    ('Olsztyn',              'Warmian-Masurian',      53.7784, 20.4801),
    ('Bielsko-Biala',        'Silesian',              49.8224, 19.0586),
    ('Bytom',                'Silesian',              50.3483, 18.9157),
    ('Zielona Gora',         'Lubusz',                51.9356, 15.5062),
    ('Rybnik',               'Silesian',              50.1022, 18.5463),
    ('Ruda Slaska',          'Silesian',              50.2558, 18.8556),
    ('Opole',                'Opole',                 50.6751, 17.9213),
    ('Tychy',                'Silesian',              50.1357, 18.9936),
    ('Gorzow Wielkopolski',  'Lubusz',                52.7325, 15.2369),
    ('Elblag',               'Warmian-Masurian',      54.1522, 19.4088),
    ('Plock',                'Masovian',              52.5463, 19.7065),
    ('Dabrowa Gornicza',     'Silesian',              50.3217, 19.1880),
    ('Walbrzych',            'Lower Silesian',        50.7714, 16.2843),
    ('Wloclawek',            'Kuyavian-Pomeranian',   52.6483, 19.0677),
    ('Tarnow',               'Lesser Poland',         50.0121, 20.9858),
    ('Chorzow',              'Silesian',              50.2975, 18.9545),
    ('Koszalin',             'West Pomeranian',       54.1943, 16.1715),
    ('Kalisz',               'Greater Poland',        51.7611, 18.0909),
    ('Legnica',              'Lower Silesian',        51.2070, 16.1619),
    ('Grudziadz',            'Kuyavian-Pomeranian',   53.4837, 18.7536),
    ('Jaworzno',             'Silesian',              50.2040, 19.2747),
    ('Slupsk',               'Pomeranian',            54.4641, 17.0285),
    ('Jastrzebie-Zdroj',     'Silesian',              49.9477, 18.5963),
    ('Nowy Sacz',            'Lesser Poland',         49.6249, 20.6915),
    ('Jelenia Gora',         'Lower Silesian',        50.9044, 15.7197),
    ('Siedlce',              'Masovian',              52.1676, 22.2903),
    ('Myslowice',            'Silesian',              50.2083, 19.1666),
    ('Konin',                'Greater Poland',        52.2230, 18.2511),
    ('Pila',                 'Greater Poland',        53.1510, 16.7382),
    ('Piotrkow Trybunalski', 'Lodz',                  51.4053, 19.7031),
    ('Inowroclaw',           'Kuyavian-Pomeranian',   52.7936, 18.2614),
    ('Lubin',                'Lower Silesian',        51.4010, 16.2015),
    ('Ostrow Wielkopolski',  'Greater Poland',        51.6550, 17.8068),
    ('Suwalki',              'Podlaskie',             54.1118, 22.9308),
    ('Stargard',             'West Pomeranian',       53.3364, 15.0502),
    ('Gniezno',              'Greater Poland',        52.5348, 17.5827),
    ('Ostrowiec Swietokrzyski', 'Holy Cross',         50.9295, 21.3856),
    ('Siemianowice Slaskie', 'Silesian',              50.3264, 19.0296),
    ('Glogow',               'Lower Silesian',        51.6634, 16.0845),
    ('Pabianice',            'Lodz',                  51.6649, 19.3548),
    ('Leszno',               'Greater Poland',        51.8425, 16.5749),
    ('Zory',                 'Silesian',              50.0455, 18.7005),
    ('Pruszkow',             'Masovian',              52.1707, 20.8120),
    ('Stalowa Wola',         'Subcarpathian',         50.5828, 22.0531),
    ('Zamosc',               'Lublin',                50.7230, 23.2519),
    ('Lomza',                'Podlaskie',             53.1784, 22.0593),
    ('Mielec',               'Subcarpathian',         50.2874, 21.4260),
    ('Tczew',                'Pomeranian',            54.0927, 18.7955),
    ('Chelm',                'Lublin',                51.1431, 23.4716),
    ('Przemysl',             'Subcarpathian',         49.7838, 22.7678),
    ('Starachowice',         'Holy Cross',            51.0378, 21.0714),
    ('Wejherowo',            'Pomeranian',            54.6059, 18.2354),
    ('Pulawy',               'Lublin',                51.4166, 21.9686),
    ('Skierniewice',         'Lodz',                  51.9542, 20.1576),
    ('Skarzysko-Kamienna',   'Holy Cross',            51.1141, 20.8597),
    ('Tarnobrzeg',           'Subcarpathian',         50.5731, 21.6792),
    ('Radomsko',             'Lodz',                  51.0671, 19.4462),
    ('Kedzierzyn-Kozle',     'Opole',                 50.3494, 18.2074),
    ('Biala Podlaska',       'Lublin',                52.0326, 23.1166),
    ('Oswiecim',             'Lesser Poland',         50.0343, 19.2098),
    ('Sandomierz',           'Holy Cross',            50.6827, 21.7489),
    ('Busko-Zdroj',          'Holy Cross',            50.4710, 20.7192),
    ('Nowa Sol',             'Lubusz',                51.8063, 15.7146),
    ('Nysa',                 'Opole',                 50.4743, 17.3346),
    ('Otwock',               'Masovian',              52.1054, 21.2614),
    ('Szczytno',             'Warmian-Masurian',      53.5630, 20.9868),
    ('Kutno',                'Lodz',                  52.2318, 19.3569),
    ('Sanok',                'Subcarpathian',         49.5566, 22.2059),
    ('Swinoujscie',          'West Pomeranian',       53.9101, 14.2474),
    ('Swidnica',             'Lower Silesian',        50.8463, 16.4872),
    ('Chojnice',             'Pomeranian',            53.6953, 17.5551),
    ('Minsk Mazowiecki',     'Masovian',              52.1790, 21.5617),
    ('Zyrardow',             'Masovian',              52.0491, 20.4467),
    ('Wolomin',              'Masovian',              52.3461, 21.2405),
    ('Nowy Targ',            'Lesser Poland',         49.4782, 20.0323),
    ('Gizycko',              'Warmian-Masurian',      54.0380, 21.7647),
    ('Brodnica',             'Kuyavian-Pomeranian',   53.2600, 19.3954),
    ('Boleslawiec',          'Lower Silesian',        51.2622, 15.5694),
    ('Swiecie',              'Kuyavian-Pomeranian',   53.4100, 18.4316),
]

CAR_MAKES = {
    'Toyota':     ['Corolla', 'Yaris', 'RAV4', 'Camry', 'C-HR', 'Aygo', 'Hilux', 'Land Cruiser'],
    'Volkswagen': ['Golf', 'Passat', 'Polo', 'Tiguan', 'T-Roc', 'Arteon', 'Touran', 'Caddy'],
    'Skoda':      ['Octavia', 'Fabia', 'Superb', 'Kodiaq', 'Karoq', 'Kamiq', 'Scala', 'Citigo'],
    'Ford':       ['Focus', 'Fiesta', 'Mondeo', 'Kuga', 'Puma', 'EcoSport', 'Transit', 'Ranger'],
    'Opel':       ['Astra', 'Corsa', 'Insignia', 'Mokka', 'Crossland', 'Grandland', 'Combo', 'Vivaro'],
    'BMW':        ['Series 3', 'Series 5', 'X1', 'X3', 'Series 1', 'X5', 'Series 7', 'X6'],
    'Audi':       ['A3', 'A4', 'A6', 'Q3', 'Q5', 'A1', 'Q7', 'TT'],
    'Mercedes':   ['Class A', 'Class C', 'Class E', 'GLC', 'GLA', 'GLE', 'Class S', 'Sprinter'],
    'Renault':    ['Clio', 'Megane', 'Captur', 'Kadjar', 'Scenic', 'Kangoo', 'Master', 'Trafic'],
    'Hyundai':    ['i30', 'Tucson', 'i20', 'Kona', 'Santa Fe', 'i10', 'ix20', 'Ioniq'],
    'Kia':        ['Ceed', 'Sportage', 'Rio', 'Stonic', 'Sorento', 'Picanto', 'XCeed', 'Niro'],
    'Fiat':       ['500', 'Tipo', 'Panda', 'Punto', '500X', 'Ducato', 'Doblo', '500L'],
    'Peugeot':    ['208', '308', '3008', '2008', '508', '5008', 'Partner', 'Rifter'],
    'Citroen':    ['C3', 'C4', 'C5 Aircross', 'Berlingo', 'C3 Aircross', 'C1', 'Jumper', 'Jumpy'],
    'Dacia':      ['Duster', 'Sandero', 'Logan', 'Dokker', 'Lodgy', 'Spring'],
    'Nissan':     ['Qashqai', 'Juke', 'Micra', 'X-Trail', 'Navara', 'Leaf', 'Note'],
    'Honda':      ['Civic', 'CR-V', 'Jazz', 'HR-V', 'Accord', 'e'],
    'Mazda':      ['3', '6', 'CX-5', 'CX-3', 'CX-30', 'MX-5', '2'],
    'Volvo':      ['XC60', 'XC40', 'V60', 'S60', 'XC90', 'V40', 'S90'],
    'Suzuki':     ['Vitara', 'Swift', 'SX4 S-Cross', 'Ignis', 'Jimny', 'Baleno'],
}

MAKE_WEIGHTS = {
    'Toyota': 0.12, 'Volkswagen': 0.11, 'Skoda': 0.10, 'Ford': 0.08,
    'Opel': 0.08, 'BMW': 0.05, 'Audi': 0.05, 'Mercedes': 0.04,
    'Renault': 0.06, 'Hyundai': 0.06, 'Kia': 0.06, 'Fiat': 0.04,
    'Peugeot': 0.04, 'Citroen': 0.03, 'Dacia': 0.03, 'Nissan': 0.02,
    'Honda': 0.02, 'Mazda': 0.02, 'Volvo': 0.02, 'Suzuki': 0.02,
}

FUEL_TYPES   = ['petrol', 'diesel', 'LPG', 'hybrid', 'electric']
FUEL_WEIGHTS = [0.35, 0.30, 0.15, 0.15, 0.05]

COLORS = ['white', 'black', 'silver', 'grey', 'red', 'blue',
          'navy', 'green', 'brown', 'beige', 'gold', 'maroon']

PRODUCT_CATEGORIES = {
    'Oils and Fluids': [
        'Engine oil 5W-30', 'Engine oil 5W-40', 'Engine oil 10W-40',
        'Engine oil 0W-20', 'Brake fluid DOT4', 'Coolant G12',
        'Summer windscreen wash', 'Winter windscreen wash',
        'Gearbox oil', 'Power steering fluid', 'AdBlue fluid 10L',
    ],
    'Filters': [
        'Oil filter', 'Air filter', 'Cabin filter', 'Fuel filter',
        'Active carbon cabin filter', 'DPF filter', 'GPF filter',
    ],
    'Brake Pads and Discs': [
        'Brake pads front', 'Brake pads rear',
        'Brake discs front', 'Brake discs rear',
        'Brake shoes', 'Brake drums',
    ],
    'Tyres': [
        'Summer tyre 205/55 R16', 'Summer tyre 195/65 R15',
        'Summer tyre 225/45 R17', 'Winter tyre 205/55 R16',
        'Winter tyre 195/65 R15', 'Winter tyre 225/45 R17',
        'All-season tyre 205/55 R16', 'All-season tyre 195/65 R15',
    ],
    'Batteries': [
        'Battery 60Ah', 'Battery 70Ah', 'Battery 74Ah',
        'Battery 80Ah', 'Battery 100Ah', 'AGM Battery 70Ah',
    ],
    'Lighting': [
        'H7 bulb', 'H4 bulb', 'H1 bulb', 'LED H7 bulb',
        'LED H4 bulb', 'W5W bulb', 'P21W bulb',
        'D1S xenon bulb', 'D2S xenon bulb',
    ],
    'Wipers': [
        'Front left wiper blade', 'Front right wiper blade',
        'Rear wiper blade', 'Front wiper blade set',
    ],
    'Suspension System': [
        'Front shock absorber', 'Rear shock absorber', 'Suspension spring',
        'Lower control arm', 'Stabiliser link', 'Control arm bushing',
        'Tie rod end', 'Tie rod',
    ],
    'Timing System': [
        'Timing belt', 'Timing kit with water pump',
        'Multi-V belt', 'Timing belt tensioner',
        'Timing chain', 'Timing chain kit',
    ],
    'Exhaust System': [
        'Rear muffler', 'Middle muffler', 'Catalytic converter',
        'Exhaust pipe', 'DPF particulate filter',
        'Lambda sensor', 'Exhaust gasket',
    ],
    'Electrical System': [
        'Alternator', 'Starter motor', 'Ignition coil',
        'Spark plug', 'Glow plug', 'ABS sensor',
        'Temperature sensor', 'Oil pressure sensor',
    ],
    'Clutch': [
        'Clutch kit', 'Clutch disc', 'Clutch pressure plate',
        'Clutch release bearing', 'Dual mass flywheel',
    ],
    'Car Care Products': [
        'Car shampoo', 'Paint wax', 'Wheel cleaner',
        'De-icer', 'Air freshener', 'Polishing compound',
        'Upholstery cleaner', 'Seal silicone',
        'Plastic restorer', 'Anti-corrosion spray',
    ],
    'Accessories': [
        'Rubber floor mat set', 'Velour floor mat set',
        'Seat covers', 'Boot organiser', 'First aid kit',
        'Warning triangle', 'Car fire extinguisher', 'Jump leads',
        'Car compass', 'USB car charger',
        'Phone holder', 'Reversing camera', 'Parking sensors',
        'Roof rack', 'Roof box', 'Tow bar',
    ],
    'Tools': [
        'Wheel wrench', 'Hydraulic jack', 'Socket wrench set',
        'Torque wrench', 'Tyre repair kit',
    ],
}

SERVICE_CATALOGUE = [
    # (name, category, min_price_net, max_price_net, estimated_time_min)
    ('Oil and filter change',                   'Periodic Service',   80,   150,  30),
    ('Periodic service',                         'Periodic Service',  150,   350,  60),
    ('Air filter replacement',                   'Periodic Service',   30,    60,  15),
    ('Cabin filter replacement',                 'Periodic Service',   30,    60,  15),
    ('Brake fluid replacement',                  'Periodic Service',   80,   150,  30),
    ('Coolant replacement',                      'Periodic Service',  100,   200,  45),
    ('Spark plug replacement',                   'Periodic Service',   60,   150,  30),
    ('Glow plug replacement',                    'Periodic Service',  100,   300,  60),
    ('Brake pads replacement front',             'Brakes',            100,   200,  45),
    ('Brake pads replacement rear',              'Brakes',             80,   180,  45),
    ('Brake discs and pads replacement front',   'Brakes',            200,   400,  60),
    ('Brake discs and pads replacement rear',    'Brakes',            180,   350,  60),
    ('Brake shoes replacement',                  'Brakes',            100,   200,  60),
    ('Tyre change (4 pcs)',                      'Tyres',              80,   160,  45),
    ('Wheel balancing (4 pcs)',                  'Tyres',              40,    80,  30),
    ('Tyre storage (season)',                    'Tyres',              60,   120,  15),
    ('Tyre repair',                              'Tyres',              20,    50,  20),
    ('Wheel alignment',                          'Tyres',             100,   200,  45),
    ('Front shock absorber replacement',         'Suspension',        200,   500, 120),
    ('Rear shock absorber replacement',          'Suspension',        150,   400,  90),
    ('Control arm replacement',                  'Suspension',        150,   350,  90),
    ('Stabiliser link replacement',              'Suspension',         50,   120,  30),
    ('Tie rod end replacement',                  'Suspension',         80,   180,  45),
    ('Timing belt replacement',                  'Timing',            400,  1200, 240),
    ('Timing kit with water pump replacement',   'Timing',            600,  1800, 300),
    ('Multi-V belt replacement',                 'Timing',             80,   200,  45),
    ('Clutch replacement',                       'Clutch',            500,  1500, 360),
    ('Dual mass flywheel replacement',           'Clutch',            800,  2500, 420),
    ('Starter motor replacement',                'Electrical System', 200,   500,  90),
    ('Alternator replacement',                   'Electrical System', 250,   600,  90),
    ('Computer diagnostics',                     'Diagnostics',        50,   150,  30),
    ('Error code clearing',                      'Diagnostics',        30,    80,  15),
    ('Air conditioning check',                   'Air Conditioning',   50,   100,  30),
    ('Air conditioning service',                 'Air Conditioning',  150,   350,  60),
    ('Interior ozone treatment',                 'Air Conditioning',   50,   100,  30),
    ('Muffler replacement',                      'Exhaust System',    150,   400,  60),
    ('Catalytic converter replacement',          'Exhaust System',    500,  2000, 120),
    ('Exhaust welding',                          'Exhaust System',     50,   150,  30),
    ('Battery replacement',                      'Electrics',          30,    60,  15),
    ('Bulb replacement',                         'Electrics',          20,    80,  15),
    ('Panel painting',                           'Bodywork',          300,  1500, 480),
    ('Bodywork and paint repair',                'Bodywork',          500,  5000, 960),
    ('Paint polishing',                          'Bodywork',          200,   600, 240),
    ('PDR dent removal',                         'Bodywork',          100,   500, 120),
    ('Technical inspection',                     'Inspection',         99,    99,  30),
    ('Technical inspection + emissions test',    'Inspection',        162,   162,  45),
    ('Engine cleaning',                          'Other',              80,   200,  60),
    ('Chassis anti-corrosion treatment',         'Other',             200,   600, 120),
]

PAYMENT_METHODS  = ['cash', 'card', 'bank_transfer', 'BLIK', 'leasing', 'instalments']
PAYMENT_WEIGHTS  = [0.15, 0.40, 0.20, 0.15, 0.05, 0.05]

WORK_ORDER_STATUSES = ['new', 'in_progress', 'waiting_for_parts', 'completed', 'cancelled']
STATUS_WEIGHTS      = [0.02, 0.03, 0.01, 0.92, 0.02]

LOCATION_TYPES        = ['workshop', 'shop', 'workshop_and_shop']
LOCATION_TYPE_WEIGHTS = [0.30, 0.20, 0.50]

POSITIONS = {
    'workshop':   ['mechanic', 'senior_mechanic', 'auto_electrician', 'panel_beater', 'painter', 'diagnostician'],
    'shop':       ['sales_assistant', 'senior_sales_assistant', 'cashier', 'warehouse_operative'],
    'management': ['branch_manager', 'deputy_manager', 'accountant'],
}

print(f'Loaded reference data:')
print(f'  - {len(CITIES)} cities')
print(f'  - {len(CAR_MAKES)} car makes')
print(f'  - {sum(len(v) for v in PRODUCT_CATEGORIES.values())} products in {len(PRODUCT_CATEGORIES)} categories')
print(f'  - {len(SERVICE_CATALOGUE)} workshop services')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dimension Tables

# COMMAND ----------

# ============================================================
# dim_locations - 100 workshop/shop locations
# ============================================================
print('Generating dim_locations...')

locations = []
for i, (city, region, lat, lon) in enumerate(CITIES[:NUM_LOCATIONS]):
    loc_type = np.random.choice(LOCATION_TYPES, p=LOCATION_TYPE_WEIGHTS)
    opening  = fake.date_between(start_date=date(2005, 1, 1), end_date=date(2020, 6, 30))
    locations.append({
        'location_id':    i + 1,
        'location_code':  f'LOC-{i+1:03d}',
        'name':           f'AutoService {city}',
        'type':           loc_type,
        'street':         fake.street_address(),
        'city':           city,
        'region':         region,
        'postal_code':    fake.postcode(),
        'latitude':       lat + np.random.uniform(-0.02, 0.02),
        'longitude':      lon + np.random.uniform(-0.02, 0.02),
        'phone':          fake.phone_number(),
        'email':          f'service.{city.lower().replace(" ", "").replace("-", "")}@autoservice.pl',
        'manager_id':     None,
        'number_of_bays': int(np.random.randint(4, 12)) if loc_type != 'shop' else 0,
        'area_m2':        int(np.random.randint(200, 800)),
        'opening_date':   opening,
        'is_active':      True if i < 95 else False,
    })

df_locations = pd.DataFrame(locations)
save_table(df_locations, 'dim_locations')
display(df_locations.head())

# COMMAND ----------

# ============================================================
# dim_employees - employees (~20 per location = ~2000)
# ============================================================
print('Generating dim_employees...')

employees = []
emp_id = 1

for _, loc in df_locations.iterrows():
    loc_id   = loc['location_id']
    loc_type = loc['type']
    open_dt  = loc['opening_date']  # datetime.date from Faker

    for position in POSITIONS['management']:
        hire_end = min(open_dt + timedelta(days=365), DATE_END)
        employees.append({
            'employee_id':       emp_id,
            'employee_code':     f'EMP-{emp_id:05d}',
            'first_name':        fake.first_name(),
            'last_name':         fake.last_name(),
            'national_id':       fake.pesel(),
            'position':          position,
            'location_id':       loc_id,
            'hire_date':         fake.date_between(start_date=open_dt, end_date=hire_end),
            'termination_date':  None,
            'hourly_rate':       round(np.random.uniform(45, 80), 2),
            'is_active':         loc['is_active'],
        })
        emp_id += 1

    if loc_type in ('workshop', 'workshop_and_shop'):
        for _ in range(np.random.randint(5, 10)):
            position = random.choice(POSITIONS['workshop'])
            employees.append({
                'employee_id':      emp_id,
                'employee_code':    f'EMP-{emp_id:05d}',
                'first_name':       fake.first_name_male() if random.random() < 0.9 else fake.first_name_female(),
                'last_name':        fake.last_name(),
                'national_id':      fake.pesel(),
                'position':         position,
                'location_id':      loc_id,
                'hire_date':        fake.date_between(start_date=open_dt, end_date=DATE_END),
                'termination_date': fake.date_between(start_date=date(2022, 1, 1), end_date=DATE_END) if random.random() < 0.1 else None,
                'hourly_rate':      round(np.random.uniform(30, 65), 2),
                'is_active':        random.random() > 0.1,
            })
            emp_id += 1

    if loc_type in ('shop', 'workshop_and_shop'):
        for _ in range(np.random.randint(3, 7)):
            position = random.choice(POSITIONS['shop'])
            employees.append({
                'employee_id':      emp_id,
                'employee_code':    f'EMP-{emp_id:05d}',
                'first_name':       fake.first_name(),
                'last_name':        fake.last_name(),
                'national_id':      fake.pesel(),
                'position':         position,
                'location_id':      loc_id,
                'hire_date':        fake.date_between(start_date=open_dt, end_date=DATE_END),
                'termination_date': fake.date_between(start_date=date(2022, 1, 1), end_date=DATE_END) if random.random() < 0.15 else None,
                'hourly_rate':      round(np.random.uniform(25, 45), 2),
                'is_active':        random.random() > 0.12,
            })
            emp_id += 1

df_employees = pd.DataFrame(employees)
save_table(df_employees, 'dim_employees')

mechanic_ids = df_employees[df_employees['position'].isin(POSITIONS['workshop'])]['employee_id'].values
seller_ids   = df_employees[df_employees['position'].isin(POSITIONS['shop'])]['employee_id'].values
loc_mechanics = df_employees[df_employees['position'].isin(POSITIONS['workshop'])].groupby('location_id')['employee_id'].apply(list).to_dict()
loc_sellers   = df_employees[df_employees['position'].isin(POSITIONS['shop'])].groupby('location_id')['employee_id'].apply(list).to_dict()

print(f'  Mechanics: {len(mechanic_ids)}, Sales staff: {len(seller_ids)}')
display(df_employees.head())

# COMMAND ----------

# ============================================================
# dim_customers - customers (500K * SCALE_FACTOR)
# ============================================================
NUM_CUSTOMERS = int(500_000 * SCALE_FACTOR)
print(f'Generating dim_customers ({NUM_CUSTOMERS:,} customers)...')

customer_types = np.random.choice(['individual', 'business'], size=NUM_CUSTOMERS, p=[0.7, 0.3])

df_customers = pd.DataFrame({
    'customer_id':           np.arange(1, NUM_CUSTOMERS + 1),
    'customer_code':         [f'CUS-{i:07d}' for i in range(1, NUM_CUSTOMERS + 1)],
    'customer_type':         customer_types,
    'first_name':            [fake.first_name() if t == 'individual' else '' for t in customer_types],
    'last_name':             [fake.last_name()  if t == 'individual' else '' for t in customer_types],
    'company_name':          [fake.company()    if t == 'business'   else '' for t in customer_types],
    'tax_id':                [fake.company_vat() if t == 'business'  else '' for t in customer_types],
    'email':                 [fake.email()        for _ in range(NUM_CUSTOMERS)],
    'phone':                 [fake.phone_number() for _ in range(NUM_CUSTOMERS)],
    'city':                  np.random.choice([m[0] for m in CITIES], size=NUM_CUSTOMERS),
    'postal_code':           [fake.postcode() for _ in range(NUM_CUSTOMERS)],
    'registration_date':     random_dates(DATE_START, DATE_END, NUM_CUSTOMERS),
    'preferred_location_id': np.random.randint(1, NUM_LOCATIONS + 1, size=NUM_CUSTOMERS),
    'marketing_consent':     np.random.choice([True, False], size=NUM_CUSTOMERS, p=[0.6, 0.4]),
})

save_table(df_customers, 'dim_customers')
customer_ids = df_customers['customer_id'].values
print(f'  Individual: {(df_customers["customer_type"] == "individual").sum():,}')
print(f'  Business:   {(df_customers["customer_type"] == "business").sum():,}')

# COMMAND ----------

# ============================================================
# dim_vehicles - customer vehicles (600K * SCALE_FACTOR)
# ============================================================
NUM_VEHICLES = int(600_000 * SCALE_FACTOR)
print(f'Generating dim_vehicles ({NUM_VEHICLES:,} vehicles)...')

makes = list(CAR_MAKES.keys())
weights_norm = np.array([MAKE_WEIGHTS[m] for m in makes])
weights_norm = weights_norm / weights_norm.sum()

chosen_makes  = np.random.choice(makes, size=NUM_VEHICLES, p=weights_norm)
chosen_models = [random.choice(CAR_MAKES[m]) for m in chosen_makes]

df_vehicles = pd.DataFrame({
    'vehicle_id':              np.arange(1, NUM_VEHICLES + 1),
    'customer_id':             np.random.choice(customer_ids, size=NUM_VEHICLES),
    'make':                    chosen_makes,
    'model':                   chosen_models,
    'year':                    np.random.randint(2005, 2025, size=NUM_VEHICLES),
    'vin':                     [fake.bothify('???#########??????').upper() for _ in range(NUM_VEHICLES)],
    'registration_number':     [fake.license_plate() for _ in range(NUM_VEHICLES)],
    'fuel_type':               np.random.choice(FUEL_TYPES, size=NUM_VEHICLES, p=FUEL_WEIGHTS),
    'engine_displacement':     np.random.choice(
        [1.0, 1.2, 1.4, 1.5, 1.6, 1.8, 2.0, 2.2, 2.5, 3.0],
        size=NUM_VEHICLES,
        p=[0.05, 0.10, 0.15, 0.12, 0.18, 0.12, 0.12, 0.06, 0.05, 0.05]
    ),
    'horsepower':              np.random.randint(60, 350, size=NUM_VEHICLES),
    'color':                   np.random.choice(COLORS, size=NUM_VEHICLES),
    'mileage_km':              np.random.randint(5000, 350000, size=NUM_VEHICLES),
    'first_registration_date': random_dates(date(2005, 1, 1), DATE_END, NUM_VEHICLES),
})

save_table(df_vehicles, 'dim_vehicles')
vehicle_ids = df_vehicles['vehicle_id'].values
print(f'  Average {NUM_VEHICLES / NUM_CUSTOMERS:.1f} vehicles per customer')

# COMMAND ----------

# ============================================================
# dim_products - products/parts (~15K with variants)
# ============================================================
print('Generating dim_products...')

products = []
prod_id  = 1

for category, product_list in PRODUCT_CATEGORIES.items():
    for base_name in product_list:
        manufacturers = random.sample(
            ['Bosch', 'Continental', 'Valeo', 'Hella', 'Mann', 'Mahle', 'NGK',
             'Brembo', 'TRW', 'KYB', 'Monroe', 'Sachs', 'LuK', 'Gates',
             'SKF', 'Dayco', 'Castrol', 'Mobil', 'Shell', 'Total', 'Motul',
             'Liqui Moly', 'K2', 'Meguiars', 'Sonax', 'Goodyear', 'Michelin',
             'Continental', 'Bridgestone', 'Pirelli', 'Varta', 'Exide', 'Banner'],
            k=min(random.randint(2, 6), 32)
        )
        for manufacturer in manufacturers:
            base_price = round(np.random.uniform(5, 800), 2)
            if 'tyre' in base_name.lower():
                base_price = round(np.random.uniform(180, 600), 2)
            elif 'Battery' in base_name or 'AGM Battery' in base_name:
                base_price = round(np.random.uniform(250, 800), 2)
            elif 'Clutch kit' in base_name or 'Dual mass flywheel' in base_name:
                base_price = round(np.random.uniform(400, 2000), 2)
            elif 'shock absorber' in base_name.lower():
                base_price = round(np.random.uniform(100, 400), 2)
            elif 'filter' in base_name.lower():
                base_price = round(np.random.uniform(15, 80), 2)
            elif 'Brake pads' in base_name or 'Brake discs' in base_name:
                base_price = round(np.random.uniform(60, 300), 2)
            elif 'oil' in base_name.lower():
                base_price = round(np.random.uniform(30, 180), 2)
            elif 'bulb' in base_name.lower():
                base_price = round(np.random.uniform(8, 120), 2)

            margin = round(np.random.uniform(1.15, 1.45), 2)
            products.append({
                'product_id':         prod_id,
                'product_code':       f'PRD-{prod_id:06d}',
                'name':               f'{base_name} {manufacturer}',
                'category':           category,
                'manufacturer':       manufacturer,
                'purchase_price_net': base_price,
                'sale_price_net':     round(base_price * margin, 2),
                'vat_rate':           23,
                'unit':               'L' if category == 'Oils and Fluids' else 'pcs',
                'weight_kg':          round(np.random.uniform(0.1, 15), 2),
                'min_stock_level':    int(np.random.randint(2, 20)),
                'is_active':          random.random() > 0.05,
            })
            prod_id += 1

df_products = pd.DataFrame(products)
save_table(df_products, 'dim_products')
product_ids = df_products['product_id'].values
print(f'  Products: {len(df_products):,} in {len(PRODUCT_CATEGORIES)} categories')

# COMMAND ----------

# ============================================================
# dim_services - workshop service catalogue
# ============================================================
print('Generating dim_services...')

services = []
for i, (name, category, min_p, max_p, duration) in enumerate(SERVICE_CATALOGUE):
    services.append({
        'service_id':         i + 1,
        'service_code':       f'SRV-{i+1:03d}',
        'name':               name,
        'category':           category,
        'min_price_net':      min_p,
        'max_price_net':      max_p,
        'estimated_time_min': duration,
        'is_active':          True,
    })

df_services = pd.DataFrame(services)
save_table(df_services, 'dim_services')
service_ids = df_services['service_id'].values

# ============================================================
# dim_suppliers - parts suppliers (~300)
# ============================================================
print('Generating dim_suppliers...')

NUM_SUPPLIERS = 300
suppliers = []
for i in range(NUM_SUPPLIERS):
    suppliers.append({
        'supplier_id':        i + 1,
        'supplier_code':      f'SUP-{i+1:04d}',
        'name':               fake.company(),
        'tax_id':             fake.company_vat(),
        'city':               random.choice([m[0] for m in CITIES]),
        'address':            fake.street_address(),
        'postal_code':        fake.postcode(),
        'phone':              fake.phone_number(),
        'email':              fake.company_email(),
        'contact_person':     fake.name(),
        'payment_terms_days': random.choice([14, 21, 30, 45, 60]),
        'min_order_value':    round(np.random.uniform(200, 2000), 2),
        'is_active':          random.random() > 0.08,
    })

df_suppliers = pd.DataFrame(suppliers)
save_table(df_suppliers, 'dim_suppliers')
supplier_ids = df_suppliers['supplier_id'].values

print('=== DIMENSION TABLES SUMMARY ===')
for name, df in [
    ('dim_locations', df_locations), ('dim_employees', df_employees),
    ('dim_customers', df_customers), ('dim_vehicles', df_vehicles),
    ('dim_products', df_products),   ('dim_services', df_services),
    ('dim_suppliers', df_suppliers),
]:
    print(f'  {name}: {len(df):,} rows')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fact Tables – Work Orders

# COMMAND ----------

# ============================================================
# fact_work_orders (~700K/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_WORK_ORDERS = int(700_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_work_orders ({NUM_WORK_ORDERS:,} work orders)...')

workshop_locs = df_locations[df_locations['type'].isin(['workshop', 'workshop_and_shop'])]['location_id'].values

def generate_work_orders_chunk(chunk_size, offset):
    dates   = seasonal_dates(DATE_START, DATE_END, chunk_size)
    loc_ids = np.random.choice(workshop_locs, size=chunk_size)
    mech_ids = [random.choice(loc_mechanics.get(lid, list(mechanic_ids[:5]))) for lid in loc_ids]
    return pd.DataFrame({
        'work_order_id':       np.arange(offset + 1, offset + chunk_size + 1),
        'work_order_code':     [f'WO-{i:08d}' for i in range(offset + 1, offset + chunk_size + 1)],
        'location_id':         loc_ids,
        'customer_id':         np.random.choice(customer_ids, size=chunk_size),
        'vehicle_id':          np.random.choice(vehicle_ids, size=chunk_size),
        'mechanic_id':         mech_ids,
        'reception_date':      dates,
        'completion_date':     dates + pd.to_timedelta(np.random.randint(0, 5, size=chunk_size), unit='D'),
        'status':              np.random.choice(WORK_ORDER_STATUSES, size=chunk_size, p=STATUS_WEIGHTS),
        'mileage_at_reception': np.random.randint(10000, 350000, size=chunk_size),
        'customer_notes':      np.random.choice(
            ['', 'Knocking noise when braking', 'Engine losing power', 'Oil leak',
             'Seasonal tyre change', 'Periodic service', 'Air conditioning not cooling',
             'Engine warning light', 'Suspension noise', 'Brake pad replacement',
             'Preparation for inspection', 'Oil change', 'Starter motor problem',
             'Steering wheel vibration', 'Spark plug replacement', ''],
            size=chunk_size
        ),
        'year':  dates.year,
        'month': dates.month,
    })

save_table_chunked(generate_work_orders_chunk, 'fact_work_orders', NUM_WORK_ORDERS,
                   partition_cols=['year', 'month'])

# COMMAND ----------

# ============================================================
# fact_work_order_items (~2.1M/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_WO_ITEMS = int(2_100_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_work_order_items ({NUM_WO_ITEMS:,} items)...')

def generate_wo_items_chunk(chunk_size, offset):
    item_types = np.random.choice(['service', 'part'], size=chunk_size, p=[0.4, 0.6])
    srv_ids  = np.where(item_types == 'service', np.random.choice(service_ids,  size=chunk_size), 0)
    prod_ids = np.where(item_types == 'part',    np.random.choice(product_ids, size=chunk_size), 0)
    quantity   = np.where(item_types == 'service', 1, np.random.randint(1, 5, size=chunk_size))
    unit_price = np.where(
        item_types == 'service',
        np.random.uniform(30, 2000, size=chunk_size),
        np.random.uniform(5,  500,  size=chunk_size),
    )
    unit_price = np.round(unit_price, 2)
    return pd.DataFrame({
        'wo_item_id':     np.arange(offset + 1, offset + chunk_size + 1),
        'work_order_id':  np.random.randint(1, NUM_WORK_ORDERS + 1, size=chunk_size),
        'item_type':      item_types,
        'service_id':     srv_ids.astype(int),
        'product_id':     prod_ids.astype(int),
        'quantity':       quantity,
        'unit_price_net': unit_price,
        'value_net':      np.round(unit_price * quantity, 2),
        'vat_rate':       23,
        'value_gross':    np.round(unit_price * quantity * 1.23, 2),
        'discount_percent': np.random.choice([0, 0, 0, 5, 10, 15], size=chunk_size),
    })

save_table_chunked(generate_wo_items_chunk, 'fact_work_order_items', NUM_WO_ITEMS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fact Tables – Retail Sales

# COMMAND ----------

# ============================================================
# fact_sales_transactions (~4.3M/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_SALES = int(4_300_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_sales_transactions ({NUM_SALES:,} transactions)...')

shop_locs = df_locations[df_locations['type'].isin(['shop', 'workshop_and_shop'])]['location_id'].values

def generate_sales_chunk(chunk_size, offset):
    dates   = seasonal_dates(DATE_START, DATE_END, chunk_size)
    hours   = np.random.choice(range(7, 20), size=chunk_size,
                                p=[0.03, 0.08, 0.10, 0.10, 0.09, 0.08, 0.08,
                                   0.08, 0.08, 0.08, 0.08, 0.07, 0.05])
    minutes    = np.random.randint(0, 60, size=chunk_size)
    timestamps = dates + pd.to_timedelta(hours, unit='h') + pd.to_timedelta(minutes, unit='m')
    loc_ids    = np.random.choice(shop_locs, size=chunk_size)
    seller_arr = [random.choice(loc_sellers.get(lid, list(seller_ids[:3]))) for lid in loc_ids]
    has_customer = np.random.random(size=chunk_size) < 0.7
    cust_ids     = np.where(has_customer, np.random.choice(customer_ids, size=chunk_size), 0)
    return pd.DataFrame({
        'transaction_id':   np.arange(offset + 1, offset + chunk_size + 1),
        'transaction_code': [f'TRX-{i:09d}' for i in range(offset + 1, offset + chunk_size + 1)],
        'location_id':      loc_ids,
        'customer_id':      cust_ids,
        'employee_id':      seller_arr,
        'transaction_date': timestamps,
        'payment_method':   np.random.choice(PAYMENT_METHODS, size=chunk_size, p=PAYMENT_WEIGHTS),
        'receipt_number':   [f'REC/{random.randint(1,999):03d}/{i+offset+1:08d}' for i in range(chunk_size)],
        'year':  dates.year,
        'month': dates.month,
    })

save_table_chunked(generate_sales_chunk, 'fact_sales_transactions', NUM_SALES,
                   partition_cols=['year', 'month'])

# COMMAND ----------

# ============================================================
# fact_sales_items (~24M/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_SALES_ITEMS = int(24_000_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_sales_items ({NUM_SALES_ITEMS:,} items)...')

def generate_sales_items_chunk(chunk_size, offset):
    quantity   = np.random.choice([1, 1, 1, 2, 2, 3, 4], size=chunk_size)
    unit_price = np.round(np.random.uniform(3, 600, size=chunk_size), 2)
    discount   = np.random.choice([0, 0, 0, 0, 5, 10, 15, 20], size=chunk_size)
    value_after_discount = np.round(unit_price * quantity * (1 - discount / 100), 2)
    return pd.DataFrame({
        'sales_item_id':  np.arange(offset + 1, offset + chunk_size + 1),
        'transaction_id': np.random.randint(1, NUM_SALES + 1, size=chunk_size),
        'product_id':     np.random.choice(product_ids, size=chunk_size),
        'quantity':       quantity,
        'unit_price_net': unit_price,
        'discount_percent': discount,
        'value_net':      value_after_discount,
        'vat_rate':       23,
        'value_gross':    np.round(value_after_discount * 1.23, 2),
    })

save_table_chunked(generate_sales_items_chunk, 'fact_sales_items', NUM_SALES_ITEMS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fact Tables – Invoices, Payments, Inventory

# COMMAND ----------

# ============================================================
# fact_invoices (~5M/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_INVOICES = int(5_000_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_invoices ({NUM_INVOICES:,} invoices)...')

def generate_invoices_chunk(chunk_size, offset):
    dates       = seasonal_dates(DATE_START, DATE_END, chunk_size)
    source_type = np.random.choice(['work_order', 'sales'], size=chunk_size, p=[0.15, 0.85])
    source_ids  = np.where(
        source_type == 'work_order',
        np.random.randint(1, max(NUM_WORK_ORDERS, 1) + 1, size=chunk_size),
        np.random.randint(1, max(NUM_SALES, 1) + 1,       size=chunk_size),
    )
    value_net = np.round(np.clip(np.random.lognormal(mean=4.5, sigma=1.0, size=chunk_size), 10, 50000), 2)
    value_vat = np.round(value_net * 0.23, 2)
    document_type = np.random.choice(
        ['vat_invoice', 'receipt', 'credit_note'], size=chunk_size, p=[0.35, 0.60, 0.05]
    )
    return pd.DataFrame({
        'invoice_id':       np.arange(offset + 1, offset + chunk_size + 1),
        'invoice_code':     [f'INV/{dates[i].year}/{i+offset+1:08d}' for i in range(chunk_size)],
        'document_type':    document_type,
        'source_type':      source_type,
        'source_id':        source_ids,
        'customer_id':      np.random.choice(customer_ids, size=chunk_size),
        'location_id':      np.random.randint(1, NUM_LOCATIONS + 1, size=chunk_size),
        'issue_date':       dates,
        'sale_date':        dates - pd.to_timedelta(np.random.randint(0, 3, size=chunk_size), unit='D'),
        'payment_due_date': dates + pd.to_timedelta(
            np.random.choice([0, 7, 14, 30], size=chunk_size, p=[0.5, 0.15, 0.2, 0.15]), unit='D'
        ),
        'value_net':   value_net,
        'value_vat':   value_vat,
        'value_gross': np.round(value_net + value_vat, 2),
        'status':      np.random.choice(
            ['paid', 'pending', 'overdue', 'cancelled'], size=chunk_size, p=[0.80, 0.10, 0.07, 0.03]
        ),
        'year':  dates.year,
        'month': dates.month,
    })

save_table_chunked(generate_invoices_chunk, 'fact_invoices', NUM_INVOICES,
                   partition_cols=['year', 'month'])

# COMMAND ----------

# ============================================================
# fact_payments (~5M/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_PAYMENTS = int(5_000_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_payments ({NUM_PAYMENTS:,} payments)...')

def generate_payments_chunk(chunk_size, offset):
    dates  = seasonal_dates(DATE_START, DATE_END, chunk_size)
    amount = np.round(np.clip(np.random.lognormal(mean=4.5, sigma=1.0, size=chunk_size), 5, 60000), 2)
    return pd.DataFrame({
        'payment_id':         np.arange(offset + 1, offset + chunk_size + 1),
        'invoice_id':         np.random.randint(1, max(NUM_INVOICES, 1) + 1, size=chunk_size),
        'payment_date':       dates,
        'amount':             amount,
        'payment_method':     np.random.choice(PAYMENT_METHODS, size=chunk_size, p=PAYMENT_WEIGHTS),
        'status':             np.random.choice(
            ['completed', 'pending', 'rejected', 'refund'], size=chunk_size, p=[0.90, 0.05, 0.03, 0.02]
        ),
        'transaction_number': [f'PAY-{uuid.uuid4().hex[:12].upper()}' for _ in range(chunk_size)],
        'year':  dates.year,
        'month': dates.month,
    })

save_table_chunked(generate_payments_chunk, 'fact_payments', NUM_PAYMENTS,
                   partition_cols=['year', 'month'])

# COMMAND ----------

# ============================================================
# fact_inventory_movements (~7M/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_INVENTORY = int(7_000_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_inventory_movements ({NUM_INVENTORY:,} movements)...')

def generate_inventory_chunk(chunk_size, offset):
    dates         = random_dates(DATE_START, DATE_END, chunk_size)
    movement_type = np.random.choice(
        ['receipt', 'issue_sales', 'issue_workshop', 'return', 'correction', 'stocktake'],
        size=chunk_size, p=[0.25, 0.35, 0.25, 0.05, 0.05, 0.05]
    )
    quantity = np.random.randint(1, 20, size=chunk_size)
    quantity = np.where(np.isin(movement_type, ['issue_sales', 'issue_workshop']), -quantity, quantity)
    return pd.DataFrame({
        'movement_id':    np.arange(offset + 1, offset + chunk_size + 1),
        'product_id':     np.random.choice(product_ids, size=chunk_size),
        'location_id':    np.random.randint(1, NUM_LOCATIONS + 1, size=chunk_size),
        'movement_type':  movement_type,
        'quantity':       quantity,
        'movement_date':  dates,
        'source_document': np.random.choice(['GR', 'GI', 'RM', 'RT', 'COR', 'INV'], size=chunk_size),
        'document_number': [f'DOC-{i+offset+1:09d}' for i in range(chunk_size)],
        'value_net':      np.round(np.abs(quantity) * np.random.uniform(5, 500, size=chunk_size), 2),
        'notes':          np.random.choice(
            ['', '', '', 'Regular delivery', 'Special order',
             'Customer return', 'Stock correction', ''],
            size=chunk_size
        ),
        'year':  dates.year,
        'month': dates.month,
    })

save_table_chunked(generate_inventory_chunk, 'fact_inventory_movements', NUM_INVENTORY,
                   partition_cols=['year', 'month'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Supporting Tables

# COMMAND ----------

# ============================================================
# fact_appointments (~700K/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_APPOINTMENTS = int(700_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_appointments ({NUM_APPOINTMENTS:,} bookings)...')

def generate_appointments_chunk(chunk_size, offset):
    dates      = seasonal_dates(DATE_START, DATE_END, chunk_size)
    hours      = np.random.choice(range(7, 17), size=chunk_size)
    timestamps = dates + pd.to_timedelta(hours, unit='h')
    return pd.DataFrame({
        'appointment_id':   np.arange(offset + 1, offset + chunk_size + 1),
        'customer_id':      np.random.choice(customer_ids, size=chunk_size),
        'vehicle_id':       np.random.choice(vehicle_ids,  size=chunk_size),
        'location_id':      np.random.choice(workshop_locs, size=chunk_size),
        'service_id':       np.random.choice(service_ids,  size=chunk_size),
        'booking_date':     dates - pd.to_timedelta(np.random.randint(1, 14, size=chunk_size), unit='D'),
        'appointment_date': timestamps,
        'status':           np.random.choice(
            ['confirmed', 'completed', 'cancelled', 'no_show'],
            size=chunk_size, p=[0.10, 0.75, 0.10, 0.05]
        ),
        'booking_channel':  np.random.choice(
            ['phone', 'online', 'in_person', 'email'],
            size=chunk_size, p=[0.35, 0.40, 0.15, 0.10]
        ),
        'notes': np.random.choice(
            ['', '', '', 'Please call before', 'Courtesy car needed',
             'Prefer morning', 'Urgent', 'Previously arranged', ''],
            size=chunk_size
        ),
        'year':  dates.year,
        'month': dates.month,
    })

save_table_chunked(generate_appointments_chunk, 'fact_appointments', NUM_APPOINTMENTS,
                   partition_cols=['year', 'month'])

# COMMAND ----------

# ============================================================
# fact_purchase_orders (~70K/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_PO = int(70_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_purchase_orders ({NUM_PO:,} orders)...')

def generate_po_chunk(chunk_size, offset):
    dates = random_dates(DATE_START, DATE_END, chunk_size)
    value = np.round(np.clip(np.random.lognormal(mean=7, sigma=0.8, size=chunk_size), 200, 100000), 2)
    return pd.DataFrame({
        'po_id':                 np.arange(offset + 1, offset + chunk_size + 1),
        'po_code':               [f'PO-{i+offset+1:07d}' for i in range(chunk_size)],
        'supplier_id':           np.random.choice(supplier_ids, size=chunk_size),
        'location_id':           np.random.randint(1, NUM_LOCATIONS + 1, size=chunk_size),
        'order_date':            dates,
        'planned_delivery_date': dates + pd.to_timedelta(np.random.randint(3, 21, size=chunk_size), unit='D'),
        'actual_delivery_date':  dates + pd.to_timedelta(np.random.randint(3, 25, size=chunk_size), unit='D'),
        'value_net':             value,
        'value_gross':           np.round(value * 1.23, 2),
        'status':                np.random.choice(
            ['placed', 'in_progress', 'delivered', 'partially_delivered', 'cancelled'],
            size=chunk_size, p=[0.03, 0.05, 0.85, 0.05, 0.02]
        ),
        'year': dates.year,
    })

save_table_chunked(generate_po_chunk, 'fact_purchase_orders', NUM_PO)

# ============================================================
# fact_purchase_order_items (~285K/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_PO_ITEMS = int(285_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_purchase_order_items ({NUM_PO_ITEMS:,} items)...')

def generate_po_items_chunk(chunk_size, offset):
    quantity = np.random.randint(1, 50, size=chunk_size)
    price    = np.round(np.random.uniform(5, 500, size=chunk_size), 2)
    return pd.DataFrame({
        'po_item_id':          np.arange(offset + 1, offset + chunk_size + 1),
        'po_id':               np.random.randint(1, max(NUM_PO, 1) + 1, size=chunk_size),
        'product_id':          np.random.choice(product_ids, size=chunk_size),
        'quantity_ordered':    quantity,
        'quantity_delivered':  np.clip(quantity + np.random.randint(-2, 1, size=chunk_size), 0, 100),
        'unit_price_net':      price,
        'value_net':           np.round(price * quantity, 2),
    })

save_table_chunked(generate_po_items_chunk, 'fact_purchase_order_items', NUM_PO_ITEMS)

# COMMAND ----------

# ============================================================
# fact_customer_feedback (~285K/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_FEEDBACK = int(285_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_customer_feedback ({NUM_FEEDBACK:,} reviews)...')

COMMENTS = [
    'Very professional service', 'Quick turnaround', 'Highly recommended!',
    'A bit overpriced', 'Long waiting time', 'Great communication',
    'Expert repair', 'Car ready ahead of schedule', 'Friendly staff',
    'Could be cheaper', 'Will definitely come back', 'Solid work',
    'Fair prices', 'Problem returned after a month', 'No complaints',
    'Excellent!', 'Average', 'Needs improvement', 'OK', '',
]

def generate_feedback_chunk(chunk_size, offset):
    dates   = random_dates(DATE_START, DATE_END, chunk_size)
    ratings = np.random.choice([1, 2, 3, 4, 5], size=chunk_size, p=[0.03, 0.05, 0.12, 0.30, 0.50])
    return pd.DataFrame({
        'feedback_id':   np.arange(offset + 1, offset + chunk_size + 1),
        'customer_id':   np.random.choice(customer_ids, size=chunk_size),
        'location_id':   np.random.randint(1, NUM_LOCATIONS + 1, size=chunk_size),
        'work_order_id': np.random.randint(1, max(NUM_WORK_ORDERS, 1) + 1, size=chunk_size),
        'feedback_date': dates,
        'rating':        ratings,
        'comment':       np.random.choice(COMMENTS, size=chunk_size),
        'category':      np.random.choice(
            ['service_quality', 'repair_quality', 'turnaround_time', 'price', 'cleanliness', 'overall'],
            size=chunk_size, p=[0.20, 0.25, 0.15, 0.15, 0.10, 0.15]
        ),
        'channel':       np.random.choice(
            ['google', 'online_form', 'email', 'phone'],
            size=chunk_size, p=[0.40, 0.30, 0.20, 0.10]
        ),
    })

save_table_chunked(generate_feedback_chunk, 'fact_customer_feedback', NUM_FEEDBACK)

# COMMAND ----------

# ============================================================
# fact_loyalty_program (500K * SCALE_FACTOR)
# ============================================================
NUM_LOYALTY = int(500_000 * SCALE_FACTOR)
print(f'Generating fact_loyalty_program ({NUM_LOYALTY:,} entries)...')

def generate_loyalty_chunk(chunk_size, offset):
    dates = random_dates(date(2021, 1, 1), DATE_END, chunk_size)
    return pd.DataFrame({
        'loyalty_id':  np.arange(offset + 1, offset + chunk_size + 1),
        'customer_id': np.random.choice(customer_ids, size=chunk_size),
        'event_date':  dates,
        'event_type':  np.random.choice(
            ['points_earned', 'points_redeemed', 'bonus', 'expiry'],
            size=chunk_size, p=[0.60, 0.20, 0.10, 0.10]
        ),
        'points':      np.random.choice(
            [-500, -200, -100, 10, 20, 50, 100, 200, 500],
            size=chunk_size, p=[0.05, 0.07, 0.08, 0.20, 0.25, 0.15, 0.10, 0.05, 0.05]
        ),
        'description': np.random.choice(
            ['Shop purchase', 'Workshop service', 'Welcome bonus',
             'Birthday bonus', 'Redeemed for 10% discount', 'Redeemed for 20% discount',
             'Redeemed for free service', 'Points expired', 'Referral bonus'],
            size=chunk_size
        ),
        'balance_after': np.random.randint(0, 5000, size=chunk_size),
        'tier':          np.random.choice(
            ['standard', 'silver', 'gold', 'platinum'],
            size=chunk_size, p=[0.50, 0.30, 0.15, 0.05]
        ),
    })

save_table_chunked(generate_loyalty_chunk, 'fact_loyalty_program', NUM_LOYALTY)

# COMMAND ----------

# ============================================================
# fact_employee_schedules (~430K/year * NUM_YEARS * SCALE_FACTOR)
# ============================================================
NUM_SCHEDULES = int(430_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_employee_schedules ({NUM_SCHEDULES:,} entries)...')

all_employee_ids = df_employees['employee_id'].values

def generate_schedules_chunk(chunk_size, offset):
    dates      = random_dates(DATE_START, DATE_END, chunk_size)
    start_hour = np.random.choice([6, 7, 8, 9, 10, 12, 14], size=chunk_size,
                                   p=[0.05, 0.25, 0.30, 0.15, 0.05, 0.10, 0.10])
    work_hours = np.random.choice([4, 6, 8, 10, 12], size=chunk_size,
                                   p=[0.10, 0.10, 0.60, 0.15, 0.05])
    return pd.DataFrame({
        'schedule_id':  np.arange(offset + 1, offset + chunk_size + 1),
        'employee_id':  np.random.choice(all_employee_ids, size=chunk_size),
        'date':         dates,
        'start_hour':   start_hour,
        'end_hour':     start_hour + work_hours,
        'shift_type':   np.random.choice(
            ['day', 'morning', 'afternoon', 'night', 'day_off', 'holiday', 'sick_leave'],
            size=chunk_size, p=[0.40, 0.15, 0.15, 0.02, 0.15, 0.08, 0.05]
        ),
        'overtime_hours': np.random.choice([0, 0, 0, 0, 0, 1, 2, 3, 4], size=chunk_size),
        'attendance':     np.random.choice(
            ['present', 'absent_excused', 'absent_unexcused', 'late'],
            size=chunk_size, p=[0.88, 0.08, 0.02, 0.02]
        ),
    })

save_table_chunked(generate_schedules_chunk, 'fact_employee_schedules', NUM_SCHEDULES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation and Statistics

# COMMAND ----------

print('=' * 60)
print('DATA GENERATION SUMMARY')
print('=' * 60)
print(f'SCALE_FACTOR    = {SCALE_FACTOR}')
print(f'OUTPUT_DIR_DIM  = {OUTPUT_DIR_DIM}')
print(f'OUTPUT_DIR_FACT = {OUTPUT_DIR_FACT}')
print()

print('Dimension tables:')
for name, df in [
    ('dim_locations', df_locations), ('dim_employees', df_employees),
    ('dim_customers', df_customers), ('dim_vehicles',  df_vehicles),
    ('dim_products',  df_products),  ('dim_services',  df_services),
    ('dim_suppliers', df_suppliers),
]:
    print(f'  {name}: {len(df):,} rows')

print()
print('Fact tables (target row counts):')
for name, n in [
    ('fact_work_orders',          NUM_WORK_ORDERS),
    ('fact_work_order_items',     NUM_WO_ITEMS),
    ('fact_sales_transactions',   NUM_SALES),
    ('fact_sales_items',          NUM_SALES_ITEMS),
    ('fact_invoices',             NUM_INVOICES),
    ('fact_payments',             NUM_PAYMENTS),
    ('fact_inventory_movements',  NUM_INVENTORY),
    ('fact_appointments',         NUM_APPOINTMENTS),
    ('fact_purchase_orders',      NUM_PO),
    ('fact_purchase_order_items', NUM_PO_ITEMS),
    ('fact_customer_feedback',    NUM_FEEDBACK),
    ('fact_loyalty_program',      NUM_LOYALTY),
    ('fact_employee_schedules',   NUM_SCHEDULES),
]:
    print(f'  {name}: {n:,}')

print()
print('Volume paths written to:')
for path in [OUTPUT_DIR_DIM, OUTPUT_DIR_FACT]:
    try:
        files = dbutils.fs.ls(path)
        total_mb = sum(f.size for f in files) / 1024 / 1024
        print(f'  {path}: {len(files)} tables, {total_mb:.1f} MB (top-level)')
    except Exception:
        print(f'  {path}: (listing unavailable)')
