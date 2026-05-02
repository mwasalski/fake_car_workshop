# Databricks notebook source

# MAGIC %md
# MAGIC # Data Generator – Car Workshop & Accessories Shop Network

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import timedelta, date
from faker import Faker
import random
import uuid
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import LongType, DoubleType, IntegerType

fake = Faker('pl_PL')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

print('Libraries loaded OK')

# COMMAND ----------

dbutils.widgets.dropdown('SINGLE_DAY_MODE', 'False', ['False', 'True'], label='Single Day Mode')
dbutils.widgets.text('TARGET_DATE', str(date.today()), label='Target Date (YYYY-MM-DD)')
_SINGLE_DAY_MODE = dbutils.widgets.get('SINGLE_DAY_MODE') == 'True'
_TARGET_DATE_STR  = dbutils.widgets.get('TARGET_DATE')

print(f'SINGLE_DAY_MODE = {_SINGLE_DAY_MODE}')

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

SCALE_FACTOR = 1.0

OUTPUT_DIR_DIM  = '/Volumes/fake_car_workshop_franchise/dim/dim_parquet_files'
OUTPUT_DIR_FACT = '/Volumes/fake_car_workshop_franchise/fact/fact_parquet_files'

DATE_START = date(2025, 1, 1)
DATE_END   = date(2026, 5, 1)

if _SINGLE_DAY_MODE:
    DATE_START = date.fromisoformat(_TARGET_DATE_STR)
    DATE_END   = date.fromisoformat(_TARGET_DATE_STR)
    print(f'SINGLE_DAY_MODE active → {DATE_START}')

NUM_YEARS     = max(DATE_END.year - DATE_START.year + 1, 1)
NUM_LOCATIONS = 100

print(f'SCALE_FACTOR = {SCALE_FACTOR}  |  {DATE_START} → {DATE_END}  |  NUM_YEARS={NUM_YEARS}')
print(f'Estimated size: ~{SCALE_FACTOR * NUM_YEARS / 7 * 30:.1f} GB')

# COMMAND ----------

# ============================================================
# HELPERS
# ============================================================

def _get_base_dir(table_name):
    return OUTPUT_DIR_DIM if table_name.startswith('dim_') else OUTPUT_DIR_FACT


def save_table(sdf, table_name, partition_cols=None):
    path = f"{_get_base_dir(table_name)}/{table_name}"
    w = sdf.write.mode('overwrite')
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.parquet(path)
    print(f'  ✓ {table_name}')


def random_date_col(start=None, end=None):
    """Uniform random date column (uses fresh F.rand() each call)."""
    s = start or DATE_START
    e = end or DATE_END
    delta = max((pd.Timestamp(e) - pd.Timestamp(s)).days, 1)
    return F.date_add(F.lit(s.isoformat()), (F.rand() * delta).cast('int'))


def wchoice(rand_col, choices, weights):
    """Weighted random choice column expression from a pre-computed rand column."""
    cum = np.cumsum(weights).tolist()
    expr = F.lit(choices[-1])
    for i in range(len(choices) - 2, -1, -1):
        expr = F.when(rand_col < float(cum[i]), F.lit(choices[i])).otherwise(expr)
    return expr


print('Helpers OK')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reference Data

# COMMAND ----------

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
    'Filters': ['Oil filter', 'Air filter', 'Cabin filter', 'Fuel filter',
                'Active carbon cabin filter', 'DPF filter', 'GPF filter'],
    'Brake Pads and Discs': ['Brake pads front', 'Brake pads rear',
                              'Brake discs front', 'Brake discs rear', 'Brake shoes', 'Brake drums'],
    'Tyres': ['Summer tyre 205/55 R16', 'Summer tyre 195/65 R15', 'Summer tyre 225/45 R17',
              'Winter tyre 205/55 R16', 'Winter tyre 195/65 R15', 'Winter tyre 225/45 R17',
              'All-season tyre 205/55 R16', 'All-season tyre 195/65 R15'],
    'Batteries': ['Battery 60Ah', 'Battery 70Ah', 'Battery 74Ah',
                  'Battery 80Ah', 'Battery 100Ah', 'AGM Battery 70Ah'],
    'Lighting': ['H7 bulb', 'H4 bulb', 'H1 bulb', 'LED H7 bulb', 'LED H4 bulb',
                 'W5W bulb', 'P21W bulb', 'D1S xenon bulb', 'D2S xenon bulb'],
    'Wipers': ['Front left wiper blade', 'Front right wiper blade',
               'Rear wiper blade', 'Front wiper blade set'],
    'Suspension System': ['Front shock absorber', 'Rear shock absorber', 'Suspension spring',
                          'Lower control arm', 'Stabiliser link', 'Control arm bushing',
                          'Tie rod end', 'Tie rod'],
    'Timing System': ['Timing belt', 'Timing kit with water pump', 'Multi-V belt',
                      'Timing belt tensioner', 'Timing chain', 'Timing chain kit'],
    'Exhaust System': ['Rear muffler', 'Middle muffler', 'Catalytic converter',
                       'Exhaust pipe', 'DPF particulate filter', 'Lambda sensor', 'Exhaust gasket'],
    'Electrical System': ['Alternator', 'Starter motor', 'Ignition coil', 'Spark plug',
                          'Glow plug', 'ABS sensor', 'Temperature sensor', 'Oil pressure sensor'],
    'Clutch': ['Clutch kit', 'Clutch disc', 'Clutch pressure plate',
               'Clutch release bearing', 'Dual mass flywheel'],
    'Car Care Products': ['Car shampoo', 'Paint wax', 'Wheel cleaner', 'De-icer', 'Air freshener',
                          'Polishing compound', 'Upholstery cleaner', 'Seal silicone',
                          'Plastic restorer', 'Anti-corrosion spray'],
    'Accessories': ['Rubber floor mat set', 'Velour floor mat set', 'Seat covers',
                    'Boot organiser', 'First aid kit', 'Warning triangle',
                    'Car fire extinguisher', 'Jump leads', 'Car compass', 'USB car charger',
                    'Phone holder', 'Reversing camera', 'Parking sensors',
                    'Roof rack', 'Roof box', 'Tow bar'],
    'Tools': ['Wheel wrench', 'Hydraulic jack', 'Socket wrench set', 'Torque wrench', 'Tyre repair kit'],
}

SERVICE_CATALOGUE = [
    ('Oil and filter change',                  'Periodic Service',   80,   150,  30),
    ('Periodic service',                        'Periodic Service',  150,   350,  60),
    ('Air filter replacement',                  'Periodic Service',   30,    60,  15),
    ('Cabin filter replacement',                'Periodic Service',   30,    60,  15),
    ('Brake fluid replacement',                 'Periodic Service',   80,   150,  30),
    ('Coolant replacement',                     'Periodic Service',  100,   200,  45),
    ('Spark plug replacement',                  'Periodic Service',   60,   150,  30),
    ('Glow plug replacement',                   'Periodic Service',  100,   300,  60),
    ('Brake pads replacement front',            'Brakes',            100,   200,  45),
    ('Brake pads replacement rear',             'Brakes',             80,   180,  45),
    ('Brake discs and pads replacement front',  'Brakes',            200,   400,  60),
    ('Brake discs and pads replacement rear',   'Brakes',            180,   350,  60),
    ('Brake shoes replacement',                 'Brakes',            100,   200,  60),
    ('Tyre change (4 pcs)',                     'Tyres',              80,   160,  45),
    ('Wheel balancing (4 pcs)',                 'Tyres',              40,    80,  30),
    ('Tyre storage (season)',                   'Tyres',              60,   120,  15),
    ('Tyre repair',                             'Tyres',              20,    50,  20),
    ('Wheel alignment',                         'Tyres',             100,   200,  45),
    ('Front shock absorber replacement',        'Suspension',        200,   500, 120),
    ('Rear shock absorber replacement',         'Suspension',        150,   400,  90),
    ('Control arm replacement',                 'Suspension',        150,   350,  90),
    ('Stabiliser link replacement',             'Suspension',         50,   120,  30),
    ('Tie rod end replacement',                 'Suspension',         80,   180,  45),
    ('Timing belt replacement',                 'Timing',            400,  1200, 240),
    ('Timing kit with water pump replacement',  'Timing',            600,  1800, 300),
    ('Multi-V belt replacement',                'Timing',             80,   200,  45),
    ('Clutch replacement',                      'Clutch',            500,  1500, 360),
    ('Dual mass flywheel replacement',          'Clutch',            800,  2500, 420),
    ('Starter motor replacement',               'Electrical System', 200,   500,  90),
    ('Alternator replacement',                  'Electrical System', 250,   600,  90),
    ('Computer diagnostics',                    'Diagnostics',        50,   150,  30),
    ('Error code clearing',                     'Diagnostics',        30,    80,  15),
    ('Air conditioning check',                  'Air Conditioning',   50,   100,  30),
    ('Air conditioning service',                'Air Conditioning',  150,   350,  60),
    ('Interior ozone treatment',                'Air Conditioning',   50,   100,  30),
    ('Muffler replacement',                     'Exhaust System',    150,   400,  60),
    ('Catalytic converter replacement',         'Exhaust System',    500,  2000, 120),
    ('Exhaust welding',                         'Exhaust System',     50,   150,  30),
    ('Battery replacement',                     'Electrics',          30,    60,  15),
    ('Bulb replacement',                        'Electrics',          20,    80,  15),
    ('Panel painting',                          'Bodywork',          300,  1500, 480),
    ('Bodywork and paint repair',               'Bodywork',          500,  5000, 960),
    ('Paint polishing',                         'Bodywork',          200,   600, 240),
    ('PDR dent removal',                        'Bodywork',          100,   500, 120),
    ('Technical inspection',                    'Inspection',         99,    99,  30),
    ('Technical inspection + emissions test',   'Inspection',        162,   162,  45),
    ('Engine cleaning',                         'Other',              80,   200,  60),
    ('Chassis anti-corrosion treatment',        'Other',             200,   600, 120),
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

NUM_SERVICES  = len(SERVICE_CATALOGUE)
NUM_SUPPLIERS = 300

print('Reference data loaded OK')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dimension Tables – small tables (Python rows → createDataFrame)

# COMMAND ----------

# ============================================================
# dim_locations  (100 rows – sequential Faker loop)
# ============================================================
print('Generating dim_locations...')
loc_rows = []
for i, (city, region, lat, lon) in enumerate(CITIES[:NUM_LOCATIONS]):
    loc_type = np.random.choice(LOCATION_TYPES, p=LOCATION_TYPE_WEIGHTS)
    opening  = fake.date_between(start_date=date(2005, 1, 1), end_date=date(2020, 6, 30))
    loc_rows.append({
        'location_id':    i + 1,
        'location_code':  f'LOC-{i+1:03d}',
        'name':           f'AutoService {city}',
        'type':           loc_type,
        'street':         fake.street_address(),
        'city':           city,
        'region':         region,
        'postal_code':    fake.postcode(),
        'latitude':       float(lat + np.random.uniform(-0.02, 0.02)),
        'longitude':      float(lon + np.random.uniform(-0.02, 0.02)),
        'phone':          fake.phone_number(),
        'email':          f'service.{city.lower().replace(" ","").replace("-","")}@autoservice.pl',
        'manager_id':     None,
        'number_of_bays': int(np.random.randint(4, 12)) if loc_type != 'shop' else 0,
        'area_m2':        int(np.random.randint(200, 800)),
        'opening_date':   opening,
        'is_active':      i < 95,
    })

df_locations = spark.createDataFrame(loc_rows)
save_table(df_locations, 'dim_locations')

# build Python lookup structures needed later
workshop_locs_list = [r['location_id'] for r in loc_rows if r['type'] in ('workshop', 'workshop_and_shop')]
shop_locs_list     = [r['location_id'] for r in loc_rows if r['type'] in ('shop', 'workshop_and_shop')]

# COMMAND ----------

# ============================================================
# dim_employees  (~2000 rows – depends on location loop)
# ============================================================
print('Generating dim_employees...')
emp_rows = []
emp_id   = 1
_loc_mechanics_dict = {}
_loc_sellers_dict   = {}

for loc in loc_rows:
    loc_id   = loc['location_id']
    loc_type = loc['type']
    open_dt  = loc['opening_date']
    hire_end = min(open_dt + timedelta(days=365), DATE_END)

    for position in POSITIONS['management']:
        emp_rows.append({
            'employee_id':      emp_id,
            'employee_code':    f'EMP-{emp_id:05d}',
            'first_name':       fake.first_name(),
            'last_name':        fake.last_name(),
            'national_id':      fake.pesel(),
            'position':         position,
            'location_id':      loc_id,
            'hire_date':        fake.date_between(start_date=open_dt, end_date=hire_end),
            'termination_date': None,
            'hourly_rate':      round(np.random.uniform(45, 80), 2),
            'is_active':        loc['is_active'],
        })
        emp_id += 1

    if loc_type in ('workshop', 'workshop_and_shop'):
        _loc_mechanics_dict[loc_id] = []
        for _ in range(np.random.randint(5, 10)):
            pos = random.choice(POSITIONS['workshop'])
            emp_rows.append({
                'employee_id':      emp_id,
                'employee_code':    f'EMP-{emp_id:05d}',
                'first_name':       fake.first_name_male() if random.random() < 0.9 else fake.first_name_female(),
                'last_name':        fake.last_name(),
                'national_id':      fake.pesel(),
                'position':         pos,
                'location_id':      loc_id,
                'hire_date':        fake.date_between(start_date=open_dt, end_date=DATE_END),
                'termination_date': fake.date_between(start_date=date(2022,1,1), end_date=DATE_END) if random.random() < 0.1 else None,
                'hourly_rate':      round(np.random.uniform(30, 65), 2),
                'is_active':        random.random() > 0.1,
            })
            _loc_mechanics_dict[loc_id].append(emp_id)
            emp_id += 1

    if loc_type in ('shop', 'workshop_and_shop'):
        _loc_sellers_dict[loc_id] = []
        for _ in range(np.random.randint(3, 7)):
            pos = random.choice(POSITIONS['shop'])
            emp_rows.append({
                'employee_id':      emp_id,
                'employee_code':    f'EMP-{emp_id:05d}',
                'first_name':       fake.first_name(),
                'last_name':        fake.last_name(),
                'national_id':      fake.pesel(),
                'position':         pos,
                'location_id':      loc_id,
                'hire_date':        fake.date_between(start_date=open_dt, end_date=DATE_END),
                'termination_date': fake.date_between(start_date=date(2022,1,1), end_date=DATE_END) if random.random() < 0.15 else None,
                'hourly_rate':      round(np.random.uniform(25, 45), 2),
                'is_active':        random.random() > 0.12,
            })
            _loc_sellers_dict[loc_id].append(emp_id)
            emp_id += 1

df_employees = spark.createDataFrame(emp_rows)
save_table(df_employees, 'dim_employees')

all_emp_ids      = [r['employee_id'] for r in emp_rows]
all_mechanic_ids = [eid for lids in _loc_mechanics_dict.values() for eid in lids]
all_seller_ids   = [eid for lids in _loc_sellers_dict.values()   for eid in lids]
print(f'  Mechanics: {len(all_mechanic_ids)}, Sellers: {len(all_seller_ids)}')

# broadcast lookup dicts for use in UDFs
_workshop_locs_bc    = spark.sparkContext.broadcast(workshop_locs_list)
_shop_locs_bc        = spark.sparkContext.broadcast(shop_locs_list)
_loc_mechanics_bc    = spark.sparkContext.broadcast(_loc_mechanics_dict)
_loc_sellers_bc      = spark.sparkContext.broadcast(_loc_sellers_dict)
_all_mechanic_ids_bc = spark.sparkContext.broadcast(all_mechanic_ids[:10])
_all_seller_ids_bc   = spark.sparkContext.broadcast(all_seller_ids[:10])
_all_emp_ids_bc      = spark.sparkContext.broadcast(all_emp_ids)

# COMMAND ----------

# ============================================================
# dim_products  (~15K rows – nested loop, small enough for driver)
# ============================================================
print('Generating dim_products...')
prod_rows = []
prod_id   = 1
_mfrs = ['Bosch','Continental','Valeo','Hella','Mann','Mahle','NGK','Brembo','TRW',
         'KYB','Monroe','Sachs','LuK','Gates','SKF','Dayco','Castrol','Mobil',
         'Shell','Total','Motul','Liqui Moly','K2','Meguiars','Sonax',
         'Goodyear','Michelin','Bridgestone','Pirelli','Varta','Exide','Banner']

for category, product_list in PRODUCT_CATEGORIES.items():
    for base_name in product_list:
        for manufacturer in random.sample(_mfrs, k=min(random.randint(2, 6), len(_mfrs))):
            p = round(np.random.uniform(5, 800), 2)
            if 'tyre' in base_name.lower():             p = round(np.random.uniform(180, 600), 2)
            elif 'Battery' in base_name:                p = round(np.random.uniform(250, 800), 2)
            elif 'Clutch kit' in base_name or 'Dual mass' in base_name: p = round(np.random.uniform(400, 2000), 2)
            elif 'shock absorber' in base_name.lower(): p = round(np.random.uniform(100, 400), 2)
            elif 'filter' in base_name.lower():         p = round(np.random.uniform(15, 80), 2)
            elif 'Brake pads' in base_name or 'Brake discs' in base_name: p = round(np.random.uniform(60, 300), 2)
            elif 'oil' in base_name.lower():            p = round(np.random.uniform(30, 180), 2)
            elif 'bulb' in base_name.lower():           p = round(np.random.uniform(8, 120), 2)
            prod_rows.append({
                'product_id':         prod_id,
                'product_code':       f'PRD-{prod_id:06d}',
                'name':               f'{base_name} {manufacturer}',
                'category':           category,
                'manufacturer':       manufacturer,
                'purchase_price_net': p,
                'sale_price_net':     round(p * round(np.random.uniform(1.15, 1.45), 2), 2),
                'vat_rate':           23,
                'unit':               'L' if category == 'Oils and Fluids' else 'pcs',
                'weight_kg':          round(np.random.uniform(0.1, 15), 2),
                'min_stock_level':    int(np.random.randint(2, 20)),
                'is_active':          random.random() > 0.05,
            })
            prod_id += 1

df_products = spark.createDataFrame(prod_rows)
save_table(df_products, 'dim_products')
NUM_PRODUCTS = len(prod_rows)
print(f'  Products: {NUM_PRODUCTS:,}')

# COMMAND ----------

# dim_services & dim_suppliers  (tiny)
svc_rows = [
    {'service_id': i+1, 'service_code': f'SRV-{i+1:03d}', 'name': name,
     'category': cat, 'min_price_net': mn, 'max_price_net': mx,
     'estimated_time_min': dur, 'is_active': True}
    for i, (name, cat, mn, mx, dur) in enumerate(SERVICE_CATALOGUE)
]
df_services = spark.createDataFrame(svc_rows)
save_table(df_services, 'dim_services')

sup_rows = [
    {'supplier_id': i+1, 'supplier_code': f'SUP-{i+1:04d}',
     'name': fake.company(), 'tax_id': fake.company_vat(),
     'city': random.choice([c[0] for c in CITIES]),
     'address': fake.street_address(), 'postal_code': fake.postcode(),
     'phone': fake.phone_number(), 'email': fake.company_email(),
     'contact_person': fake.name(),
     'payment_terms_days': random.choice([14, 21, 30, 45, 60]),
     'min_order_value': round(np.random.uniform(200, 2000), 2),
     'is_active': random.random() > 0.08}
    for i in range(NUM_SUPPLIERS)
]
df_suppliers = spark.createDataFrame(sup_rows)
save_table(df_suppliers, 'dim_suppliers')

print(f'  Services: {len(svc_rows)}, Suppliers: {len(sup_rows)}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dimension Tables – large tables (spark.range + pandas_udf)

# COMMAND ----------

# ============================================================
# Reusable pandas UDFs – defined once, used across all tables
# ============================================================

@pandas_udf('string')
def udf_first_name(s: pd.Series) -> pd.Series:
    from faker import Faker as _F
    f = _F('pl_PL')
    return pd.Series([f.first_name() for _ in range(len(s))])

@pandas_udf('string')
def udf_last_name(s: pd.Series) -> pd.Series:
    from faker import Faker as _F
    f = _F('pl_PL')
    return pd.Series([f.last_name() for _ in range(len(s))])

@pandas_udf('string')
def udf_company(s: pd.Series) -> pd.Series:
    from faker import Faker as _F
    f = _F('pl_PL')
    return pd.Series([f.company() for _ in range(len(s))])

@pandas_udf('string')
def udf_vat(s: pd.Series) -> pd.Series:
    from faker import Faker as _F
    f = _F('pl_PL')
    return pd.Series([f.company_vat() for _ in range(len(s))])

@pandas_udf('string')
def udf_email(s: pd.Series) -> pd.Series:
    from faker import Faker as _F
    f = _F('pl_PL')
    return pd.Series([f.email() for _ in range(len(s))])

@pandas_udf('string')
def udf_phone(s: pd.Series) -> pd.Series:
    from faker import Faker as _F
    f = _F('pl_PL')
    return pd.Series([f.phone_number() for _ in range(len(s))])

@pandas_udf('string')
def udf_postcode(s: pd.Series) -> pd.Series:
    from faker import Faker as _F
    f = _F('pl_PL')
    return pd.Series([f.postcode() for _ in range(len(s))])

@pandas_udf('string')
def udf_vin(s: pd.Series) -> pd.Series:
    from faker import Faker as _F
    f = _F('pl_PL')
    return pd.Series([f.bothify('???#########??????').upper() for _ in range(len(s))])

@pandas_udf('string')
def udf_plate(s: pd.Series) -> pd.Series:
    from faker import Faker as _F
    f = _F('pl_PL')
    return pd.Series([f.license_plate() for _ in range(len(s))])

@pandas_udf('string')
def udf_uuid_pay(s: pd.Series) -> pd.Series:
    import uuid as _u
    return pd.Series([f'PAY-{_u.uuid4().hex[:12].upper()}' for _ in range(len(s))])

_cities_bc      = spark.sparkContext.broadcast([c[0] for c in CITIES])
_makes_list     = list(CAR_MAKES.keys())
_make_w         = np.array([MAKE_WEIGHTS[m] for m in _makes_list]); _make_w /= _make_w.sum()
_makes_bc       = spark.sparkContext.broadcast(_makes_list)
_make_w_bc      = spark.sparkContext.broadcast(_make_w.tolist())
_car_makes_bc   = spark.sparkContext.broadcast(CAR_MAKES)

@pandas_udf('string')
def udf_city_choice(s: pd.Series) -> pd.Series:
    import numpy as _np
    cities = _cities_bc.value
    return pd.Series(_np.random.choice(cities, size=len(s)))

@pandas_udf('string')
def udf_car_make(s: pd.Series) -> pd.Series:
    import numpy as _np
    makes = _makes_bc.value
    w     = _np.array(_make_w_bc.value)
    return pd.Series(_np.random.choice(makes, size=len(s), p=w))

@pandas_udf('string')
def udf_car_model(makes: pd.Series) -> pd.Series:
    import random as _r
    cm = _car_makes_bc.value
    return pd.Series([_r.choice(cm[m]) for m in makes])

@pandas_udf('date')
def udf_seasonal_date(s: pd.Series) -> pd.Series:
    import numpy as _np, pandas as _pd
    n     = len(s)
    start = _pd.Timestamp(DATE_START); end = _pd.Timestamp(DATE_END)
    delta = max((end - start).days, 1)
    dates = start + _pd.to_timedelta(_np.random.randint(0, delta, size=n), unit='D')
    sw    = {1:.7,2:.7,3:1.4,4:1.4,5:1.,6:.9,7:.8,8:.8,9:1.,10:1.4,11:1.3,12:.6}
    w     = _np.array([sw[m] for m in dates.month]); w /= w.sum()
    idx   = _np.random.choice(n, size=n, replace=True, p=w)
    return _pd.Series(dates[idx].dt.date)

@pandas_udf('long')
def udf_workshop_loc(s: pd.Series) -> pd.Series:
    import random as _r
    locs = _workshop_locs_bc.value
    return pd.Series([_r.choice(locs) for _ in range(len(s))])

@pandas_udf('long')
def udf_shop_loc(s: pd.Series) -> pd.Series:
    import random as _r
    locs = _shop_locs_bc.value
    return pd.Series([_r.choice(locs) for _ in range(len(s))])

@pandas_udf('long')
def udf_mechanic_for_loc(loc_ids: pd.Series) -> pd.Series:
    import random as _r
    lm = _loc_mechanics_bc.value; fb = _all_mechanic_ids_bc.value
    return pd.Series([_r.choice(lm.get(int(lid), fb)) for lid in loc_ids])

@pandas_udf('long')
def udf_seller_for_loc(loc_ids: pd.Series) -> pd.Series:
    import random as _r
    ls = _loc_sellers_bc.value; fb = _all_seller_ids_bc.value
    return pd.Series([_r.choice(ls.get(int(lid), fb)) for lid in loc_ids])

@pandas_udf('long')
def udf_any_employee(s: pd.Series) -> pd.Series:
    import random as _r
    ids = _all_emp_ids_bc.value
    return pd.Series([_r.choice(ids) for _ in range(len(s))])

print('UDFs registered OK')

# COMMAND ----------

# ============================================================
# dim_customers  (500K * SCALE_FACTOR)
# ============================================================
NUM_CUSTOMERS = int(500_000 * SCALE_FACTOR)
print(f'Generating dim_customers ({NUM_CUSTOMERS:,})...')

df_customers = (
    spark.range(1, NUM_CUSTOMERS + 1).withColumnRenamed('id', 'customer_id')
    .withColumn('customer_code', F.concat(F.lit('CUS-'), F.lpad(F.col('customer_id').cast('string'), 7, '0')))
    .withColumn('customer_type', F.when(F.rand() < 0.7, 'individual').otherwise('business'))
    .withColumn('first_name',  F.when(F.col('customer_type') == 'individual', udf_first_name(F.col('customer_id'))).otherwise(F.lit('')))
    .withColumn('last_name',   F.when(F.col('customer_type') == 'individual', udf_last_name(F.col('customer_id'))).otherwise(F.lit('')))
    .withColumn('company_name',F.when(F.col('customer_type') == 'business',   udf_company(F.col('customer_id'))).otherwise(F.lit('')))
    .withColumn('tax_id',      F.when(F.col('customer_type') == 'business',   udf_vat(F.col('customer_id'))).otherwise(F.lit('')))
    .withColumn('email',       udf_email(F.col('customer_id')))
    .withColumn('phone',       udf_phone(F.col('customer_id')))
    .withColumn('city',        udf_city_choice(F.col('customer_id')))
    .withColumn('postal_code', udf_postcode(F.col('customer_id')))
    .withColumn('registration_date',     random_date_col())
    .withColumn('preferred_location_id', (F.rand() * NUM_LOCATIONS).cast('long') + 1)
    .withColumn('marketing_consent',     F.rand() < 0.6)
)
save_table(df_customers, 'dim_customers')

# COMMAND ----------

# ============================================================
# dim_vehicles  (600K * SCALE_FACTOR)
# ============================================================
NUM_VEHICLES = int(600_000 * SCALE_FACTOR)
print(f'Generating dim_vehicles ({NUM_VEHICLES:,})...')

_veh_date_delta = max((pd.Timestamp(DATE_END) - pd.Timestamp(date(2005,1,1))).days, 1)

df_vehicles = (
    spark.range(1, NUM_VEHICLES + 1).withColumnRenamed('id', 'vehicle_id')
    .withColumn('customer_id', (F.rand() * NUM_CUSTOMERS).cast('long') + 1)
    .withColumn('make',  udf_car_make(F.col('vehicle_id')))
    .withColumn('model', udf_car_model(F.col('make')))
    .withColumn('year',  (F.rand() * 20 + 2005).cast('int'))
    .withColumn('vin',   udf_vin(F.col('vehicle_id')))
    .withColumn('registration_number', udf_plate(F.col('vehicle_id')))
    .withColumn('fuel_type', wchoice(F.rand(), FUEL_TYPES, FUEL_WEIGHTS))
    .withColumn('engine_displacement',
        wchoice(F.rand(),
            ['1.0','1.2','1.4','1.5','1.6','1.8','2.0','2.2','2.5','3.0'],
            [0.05,0.10,0.15,0.12,0.18,0.12,0.12,0.06,0.05,0.05]).cast('double'))
    .withColumn('horsepower',  (F.rand() * 290 + 60).cast('int'))
    .withColumn('color',       wchoice(F.rand(), COLORS, [1/len(COLORS)]*len(COLORS)))
    .withColumn('mileage_km',  (F.rand() * 345000 + 5000).cast('long'))
    .withColumn('first_registration_date',
        F.date_add(F.lit('2005-01-01'), (F.rand() * _veh_date_delta).cast('int')))
)
save_table(df_vehicles, 'dim_vehicles')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fact Tables

# COMMAND ----------

# ============================================================
# fact_work_orders
# ============================================================
NUM_WORK_ORDERS = int(700_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_work_orders ({NUM_WORK_ORDERS:,})...')

_wo_notes = ['','Knocking noise when braking','Engine losing power','Oil leak',
             'Seasonal tyre change','Periodic service','Air conditioning not cooling',
             'Engine warning light','Suspension noise','Brake pad replacement',
             'Preparation for inspection','Oil change','Starter motor problem',
             'Steering wheel vibration','Spark plug replacement','']
_wo_note_w = [1/len(_wo_notes)] * len(_wo_notes)

df_work_orders = (
    spark.range(1, NUM_WORK_ORDERS + 1).withColumnRenamed('id', 'work_order_id')
    .withColumn('work_order_code', F.concat(F.lit('WO-'), F.lpad(F.col('work_order_id').cast('string'), 8, '0')))
    .withColumn('location_id',  udf_workshop_loc(F.col('work_order_id')))
    .withColumn('customer_id',  (F.rand() * NUM_CUSTOMERS).cast('long') + 1)
    .withColumn('vehicle_id',   (F.rand() * NUM_VEHICLES).cast('long') + 1)
    .withColumn('mechanic_id',  udf_mechanic_for_loc(F.col('location_id')))
    .withColumn('reception_date',   udf_seasonal_date(F.col('work_order_id')))
    .withColumn('completion_date',  F.date_add(F.col('reception_date'), (F.rand() * 5).cast('int')))
    .withColumn('status',           wchoice(F.rand(), WORK_ORDER_STATUSES, STATUS_WEIGHTS))
    .withColumn('mileage_at_reception', (F.rand() * 340000 + 10000).cast('long'))
    .withColumn('customer_notes',   wchoice(F.rand(), _wo_notes, _wo_note_w))
    .withColumn('year',  F.year('reception_date'))
    .withColumn('month', F.month('reception_date'))
)
save_table(df_work_orders, 'fact_work_orders', partition_cols=['year','month'])

# COMMAND ----------

# ============================================================
# fact_work_order_items
# ============================================================
NUM_WO_ITEMS = int(2_100_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_work_order_items ({NUM_WO_ITEMS:,})...')

df_wo_items = (
    spark.range(1, NUM_WO_ITEMS + 1).withColumnRenamed('id', 'wo_item_id')
    .withColumn('work_order_id', (F.rand() * NUM_WORK_ORDERS).cast('long') + 1)
    .withColumn('item_type',     F.when(F.rand() < 0.4, 'service').otherwise('part'))
    .withColumn('service_id',    F.when(F.col('item_type') == 'service', (F.rand() * NUM_SERVICES).cast('long') + 1).otherwise(F.lit(0).cast('long')))
    .withColumn('product_id',    F.when(F.col('item_type') == 'part',    (F.rand() * NUM_PRODUCTS).cast('long') + 1).otherwise(F.lit(0).cast('long')))
    .withColumn('quantity',      F.when(F.col('item_type') == 'service', F.lit(1)).otherwise((F.rand() * 4 + 1).cast('int')))
    .withColumn('unit_price_net',F.when(F.col('item_type') == 'service', F.round(F.rand() * 1970 + 30, 2)).otherwise(F.round(F.rand() * 495 + 5, 2)))
    .withColumn('value_net',     F.round(F.col('unit_price_net') * F.col('quantity'), 2))
    .withColumn('vat_rate',      F.lit(23))
    .withColumn('value_gross',   F.round(F.col('value_net') * 1.23, 2))
    .withColumn('discount_percent', wchoice(F.rand(), [0,0,0,5,10,15], [3/6,1/6,1/6,1/6,1/6,1/6]).cast('int'))
)
save_table(df_wo_items, 'fact_work_order_items')

# COMMAND ----------

# ============================================================
# fact_sales_transactions
# ============================================================
NUM_SALES = int(4_300_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_sales_transactions ({NUM_SALES:,})...')

_hours   = list(range(7, 20))
_hour_w  = [0.03,0.08,0.10,0.10,0.09,0.08,0.08,0.08,0.08,0.08,0.08,0.07,0.05]

df_sales = (
    spark.range(1, NUM_SALES + 1).withColumnRenamed('id', 'transaction_id')
    .withColumn('transaction_code', F.concat(F.lit('TRX-'), F.lpad(F.col('transaction_id').cast('string'), 9, '0')))
    .withColumn('location_id',  udf_shop_loc(F.col('transaction_id')))
    .withColumn('customer_id',  F.when(F.rand() < 0.7, (F.rand() * NUM_CUSTOMERS).cast('long') + 1).otherwise(F.lit(0).cast('long')))
    .withColumn('employee_id',  udf_seller_for_loc(F.col('location_id')))
    .withColumn('_date',        udf_seasonal_date(F.col('transaction_id')))
    .withColumn('_hour',        wchoice(F.rand(), [str(h) for h in _hours], _hour_w).cast('int'))
    .withColumn('_min',         (F.rand() * 60).cast('int'))
    .withColumn('transaction_date', (F.col('_date').cast('timestamp') + F.expr('INTERVAL 1 HOUR') * F.col('_hour') + F.expr('INTERVAL 1 MINUTE') * F.col('_min')))
    .withColumn('payment_method',   wchoice(F.rand(), PAYMENT_METHODS, PAYMENT_WEIGHTS))
    .withColumn('receipt_number',
        F.concat(F.lit('REC/'), F.lpad((F.rand()*999+1).cast('int').cast('string'), 3, '0'),
                 F.lit('/'), F.lpad(F.col('transaction_id').cast('string'), 8, '0')))
    .withColumn('year',  F.year('_date'))
    .withColumn('month', F.month('_date'))
    .drop('_date', '_hour', '_min')
)
save_table(df_sales, 'fact_sales_transactions', partition_cols=['year','month'])

# COMMAND ----------

# ============================================================
# fact_sales_items
# ============================================================
NUM_SALES_ITEMS = int(24_000_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_sales_items ({NUM_SALES_ITEMS:,})...')

df_sales_items = (
    spark.range(1, NUM_SALES_ITEMS + 1).withColumnRenamed('id', 'sales_item_id')
    .withColumn('transaction_id', (F.rand() * NUM_SALES).cast('long') + 1)
    .withColumn('product_id',     (F.rand() * NUM_PRODUCTS).cast('long') + 1)
    .withColumn('quantity',       wchoice(F.rand(), [1,1,1,2,2,3,4], [3/7,1/7,1/7,1/7,1/7,1/7,1/7]).cast('int'))
    .withColumn('unit_price_net', F.round(F.rand() * 597 + 3, 2))
    .withColumn('discount_percent', wchoice(F.rand(), [0,0,0,0,5,10,15,20], [4/8,1/8,1/8,1/8,1/8,1/8,1/8,1/8]).cast('int'))
    .withColumn('value_net',      F.round(F.col('unit_price_net') * F.col('quantity') * (1 - F.col('discount_percent') / 100), 2))
    .withColumn('vat_rate',       F.lit(23))
    .withColumn('value_gross',    F.round(F.col('value_net') * 1.23, 2))
)
save_table(df_sales_items, 'fact_sales_items')

# COMMAND ----------

# ============================================================
# fact_invoices
# ============================================================
NUM_INVOICES = int(5_000_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_invoices ({NUM_INVOICES:,})...')

df_invoices = (
    spark.range(1, NUM_INVOICES + 1).withColumnRenamed('id', 'invoice_id')
    .withColumn('_date',        udf_seasonal_date(F.col('invoice_id')))
    .withColumn('source_type',  F.when(F.rand() < 0.15, 'work_order').otherwise('sales'))
    .withColumn('source_id',    F.when(F.col('source_type') == 'work_order', (F.rand() * NUM_WORK_ORDERS).cast('long') + 1).otherwise((F.rand() * NUM_SALES).cast('long') + 1))
    .withColumn('document_type', wchoice(F.rand(), ['vat_invoice','receipt','credit_note'], [0.35,0.60,0.05]))
    .withColumn('invoice_code', F.concat(F.lit('INV/'), F.year('_date').cast('string'), F.lit('/'), F.lpad(F.col('invoice_id').cast('string'), 8, '0')))
    .withColumn('customer_id',  (F.rand() * NUM_CUSTOMERS).cast('long') + 1)
    .withColumn('location_id',  (F.rand() * NUM_LOCATIONS).cast('long') + 1)
    .withColumn('issue_date',   F.col('_date'))
    .withColumn('sale_date',    F.date_sub(F.col('_date'), (F.rand() * 3).cast('int')))
    .withColumn('payment_due_date', F.date_add(F.col('_date'), wchoice(F.rand(), [0,7,14,30], [0.5,0.15,0.2,0.15]).cast('int')))
    .withColumn('value_net',    F.round(F.least(F.greatest(F.exp(F.randn() + 4.5), F.lit(10.0)), F.lit(50000.0)), 2))
    .withColumn('value_vat',    F.round(F.col('value_net') * 0.23, 2))
    .withColumn('value_gross',  F.round(F.col('value_net') + F.col('value_vat'), 2))
    .withColumn('status',       wchoice(F.rand(), ['paid','pending','overdue','cancelled'], [0.80,0.10,0.07,0.03]))
    .withColumn('year',  F.year('_date'))
    .withColumn('month', F.month('_date'))
    .drop('_date')
)
save_table(df_invoices, 'fact_invoices', partition_cols=['year','month'])

# COMMAND ----------

# ============================================================
# fact_payments
# ============================================================
NUM_PAYMENTS = int(5_000_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_payments ({NUM_PAYMENTS:,})...')

df_payments = (
    spark.range(1, NUM_PAYMENTS + 1).withColumnRenamed('id', 'payment_id')
    .withColumn('invoice_id',         (F.rand() * NUM_INVOICES).cast('long') + 1)
    .withColumn('payment_date',        udf_seasonal_date(F.col('payment_id')))
    .withColumn('amount',              F.round(F.least(F.greatest(F.exp(F.randn() + 4.5), F.lit(5.0)), F.lit(60000.0)), 2))
    .withColumn('payment_method',      wchoice(F.rand(), PAYMENT_METHODS, PAYMENT_WEIGHTS))
    .withColumn('status',              wchoice(F.rand(), ['completed','pending','rejected','refund'], [0.90,0.05,0.03,0.02]))
    .withColumn('transaction_number',  udf_uuid_pay(F.col('payment_id')))
    .withColumn('year',  F.year('payment_date'))
    .withColumn('month', F.month('payment_date'))
)
save_table(df_payments, 'fact_payments', partition_cols=['year','month'])

# COMMAND ----------

# ============================================================
# fact_inventory_movements
# ============================================================
NUM_INVENTORY = int(7_000_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_inventory_movements ({NUM_INVENTORY:,})...')

_inv_types = ['receipt','issue_sales','issue_workshop','return','correction','stocktake']
_inv_w     = [0.25, 0.35, 0.25, 0.05, 0.05, 0.05]
_inv_notes = ['','','','Regular delivery','Special order','Customer return','Stock correction','']
_inv_docs  = ['GR','GI','RM','RT','COR','INV']

df_inventory = (
    spark.range(1, NUM_INVENTORY + 1).withColumnRenamed('id', 'movement_id')
    .withColumn('product_id',    (F.rand() * NUM_PRODUCTS).cast('long') + 1)
    .withColumn('location_id',   (F.rand() * NUM_LOCATIONS).cast('long') + 1)
    .withColumn('movement_type', wchoice(F.rand(), _inv_types, _inv_w))
    .withColumn('_qty',          (F.rand() * 19 + 1).cast('long'))
    .withColumn('quantity',      F.when(F.col('movement_type').isin('issue_sales','issue_workshop'), -F.col('_qty')).otherwise(F.col('_qty')))
    .withColumn('movement_date', random_date_col())
    .withColumn('source_document', wchoice(F.rand(), _inv_docs, [1/6]*6))
    .withColumn('document_number', F.concat(F.lit('DOC-'), F.lpad(F.col('movement_id').cast('string'), 9, '0')))
    .withColumn('value_net',     F.round(F.abs(F.col('_qty')) * (F.rand() * 495 + 5), 2))
    .withColumn('notes',         wchoice(F.rand(), _inv_notes, [1/8]*8))
    .withColumn('year',  F.year('movement_date'))
    .withColumn('month', F.month('movement_date'))
    .drop('_qty')
)
save_table(df_inventory, 'fact_inventory_movements', partition_cols=['year','month'])

# COMMAND ----------

# ============================================================
# fact_appointments
# ============================================================
NUM_APPOINTMENTS = int(700_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_appointments ({NUM_APPOINTMENTS:,})...')

df_appointments = (
    spark.range(1, NUM_APPOINTMENTS + 1).withColumnRenamed('id', 'appointment_id')
    .withColumn('customer_id', (F.rand() * NUM_CUSTOMERS).cast('long') + 1)
    .withColumn('vehicle_id',  (F.rand() * NUM_VEHICLES).cast('long') + 1)
    .withColumn('location_id', udf_workshop_loc(F.col('appointment_id')))
    .withColumn('service_id',  (F.rand() * NUM_SERVICES).cast('long') + 1)
    .withColumn('_date',       udf_seasonal_date(F.col('appointment_id')))
    .withColumn('booking_date',     F.date_sub(F.col('_date'), (F.rand() * 13 + 1).cast('int')))
    .withColumn('appointment_date', F.col('_date').cast('timestamp') + F.expr('INTERVAL 1 HOUR') * (F.rand() * 10 + 7).cast('int'))
    .withColumn('status',        wchoice(F.rand(), ['confirmed','completed','cancelled','no_show'], [0.10,0.75,0.10,0.05]))
    .withColumn('booking_channel', wchoice(F.rand(), ['phone','online','in_person','email'], [0.35,0.40,0.15,0.10]))
    .withColumn('notes',         wchoice(F.rand(), ['','','','Please call before','Courtesy car needed','Prefer morning','Urgent','Previously arranged',''], [1/9]*9))
    .withColumn('year',  F.year('_date'))
    .withColumn('month', F.month('_date'))
    .drop('_date')
)
save_table(df_appointments, 'fact_appointments', partition_cols=['year','month'])

# COMMAND ----------

# ============================================================
# fact_purchase_orders + fact_purchase_order_items
# ============================================================
NUM_PO = int(70_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_purchase_orders ({NUM_PO:,})...')

df_po = (
    spark.range(1, NUM_PO + 1).withColumnRenamed('id', 'po_id')
    .withColumn('po_code',     F.concat(F.lit('PO-'), F.lpad(F.col('po_id').cast('string'), 7, '0')))
    .withColumn('supplier_id', (F.rand() * NUM_SUPPLIERS).cast('long') + 1)
    .withColumn('location_id', (F.rand() * NUM_LOCATIONS).cast('long') + 1)
    .withColumn('order_date',  random_date_col())
    .withColumn('planned_delivery_date', F.date_add(F.col('order_date'), (F.rand() * 18 + 3).cast('int')))
    .withColumn('actual_delivery_date',  F.date_add(F.col('order_date'), (F.rand() * 22 + 3).cast('int')))
    .withColumn('value_net',   F.round(F.least(F.greatest(F.exp(F.randn() + 7), F.lit(200.0)), F.lit(100000.0)), 2))
    .withColumn('value_gross', F.round(F.col('value_net') * 1.23, 2))
    .withColumn('status',      wchoice(F.rand(), ['placed','in_progress','delivered','partially_delivered','cancelled'], [0.03,0.05,0.85,0.05,0.02]))
    .withColumn('year',        F.year('order_date'))
)
save_table(df_po, 'fact_purchase_orders')

NUM_PO_ITEMS = int(285_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_purchase_order_items ({NUM_PO_ITEMS:,})...')

df_po_items = (
    spark.range(1, NUM_PO_ITEMS + 1).withColumnRenamed('id', 'po_item_id')
    .withColumn('po_id',              (F.rand() * NUM_PO).cast('long') + 1)
    .withColumn('product_id',         (F.rand() * NUM_PRODUCTS).cast('long') + 1)
    .withColumn('quantity_ordered',   (F.rand() * 49 + 1).cast('long'))
    .withColumn('quantity_delivered', F.greatest(F.lit(0).cast('long'), F.col('quantity_ordered') + (F.rand() * 3 - 2).cast('long')))
    .withColumn('unit_price_net',     F.round(F.rand() * 495 + 5, 2))
    .withColumn('value_net',          F.round(F.col('unit_price_net') * F.col('quantity_ordered'), 2))
)
save_table(df_po_items, 'fact_purchase_order_items')

# COMMAND ----------

# ============================================================
# fact_customer_feedback
# ============================================================
NUM_FEEDBACK = int(285_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_customer_feedback ({NUM_FEEDBACK:,})...')

_comments = ['Very professional service','Quick turnaround','Highly recommended!',
             'A bit overpriced','Long waiting time','Great communication',
             'Expert repair','Car ready ahead of schedule','Friendly staff',
             'Could be cheaper','Will definitely come back','Solid work',
             'Fair prices','Problem returned after a month','No complaints',
             'Excellent!','Average','Needs improvement','OK','']

df_feedback = (
    spark.range(1, NUM_FEEDBACK + 1).withColumnRenamed('id', 'feedback_id')
    .withColumn('customer_id',   (F.rand() * NUM_CUSTOMERS).cast('long') + 1)
    .withColumn('location_id',   (F.rand() * NUM_LOCATIONS).cast('long') + 1)
    .withColumn('work_order_id', (F.rand() * NUM_WORK_ORDERS).cast('long') + 1)
    .withColumn('feedback_date', random_date_col())
    .withColumn('rating',        wchoice(F.rand(), [1,2,3,4,5], [0.03,0.05,0.12,0.30,0.50]).cast('int'))
    .withColumn('comment',       wchoice(F.rand(), _comments, [1/len(_comments)]*len(_comments)))
    .withColumn('category',      wchoice(F.rand(), ['service_quality','repair_quality','turnaround_time','price','cleanliness','overall'], [0.20,0.25,0.15,0.15,0.10,0.15]))
    .withColumn('channel',       wchoice(F.rand(), ['google','online_form','email','phone'], [0.40,0.30,0.20,0.10]))
)
save_table(df_feedback, 'fact_customer_feedback')

# COMMAND ----------

# ============================================================
# fact_loyalty_program
# ============================================================
NUM_LOYALTY = int(500_000 * SCALE_FACTOR)
print(f'Generating fact_loyalty_program ({NUM_LOYALTY:,})...')

_loyalty_delta = max((pd.Timestamp(DATE_END) - pd.Timestamp(date(2021,1,1))).days, 1)
_loy_pts = [-500,-200,-100,10,20,50,100,200,500]
_loy_pw  = [0.05,0.07,0.08,0.20,0.25,0.15,0.10,0.05,0.05]
_loy_desc = ['Shop purchase','Workshop service','Welcome bonus','Birthday bonus',
             'Redeemed for 10% discount','Redeemed for 20% discount',
             'Redeemed for free service','Points expired','Referral bonus']

df_loyalty = (
    spark.range(1, NUM_LOYALTY + 1).withColumnRenamed('id', 'loyalty_id')
    .withColumn('customer_id', (F.rand() * NUM_CUSTOMERS).cast('long') + 1)
    .withColumn('event_date',  F.date_add(F.lit('2021-01-01'), (F.rand() * _loyalty_delta).cast('int')))
    .withColumn('event_type',  wchoice(F.rand(), ['points_earned','points_redeemed','bonus','expiry'], [0.60,0.20,0.10,0.10]))
    .withColumn('points',      wchoice(F.rand(), [str(p) for p in _loy_pts], _loy_pw).cast('int'))
    .withColumn('description', wchoice(F.rand(), _loy_desc, [1/len(_loy_desc)]*len(_loy_desc)))
    .withColumn('balance_after', (F.rand() * 5000).cast('long'))
    .withColumn('tier',          wchoice(F.rand(), ['standard','silver','gold','platinum'], [0.50,0.30,0.15,0.05]))
)
save_table(df_loyalty, 'fact_loyalty_program')

# COMMAND ----------

# ============================================================
# fact_employee_schedules
# ============================================================
NUM_SCHEDULES = int(430_000 * NUM_YEARS * SCALE_FACTOR)
print(f'Generating fact_employee_schedules ({NUM_SCHEDULES:,})...')

_start_h = [6,7,8,9,10,12,14];  _start_hw = [0.05,0.25,0.30,0.15,0.05,0.10,0.10]
_work_h  = [4,6,8,10,12];       _work_hw  = [0.10,0.10,0.60,0.15,0.05]

df_schedules = (
    spark.range(1, NUM_SCHEDULES + 1).withColumnRenamed('id', 'schedule_id')
    .withColumn('employee_id',    udf_any_employee(F.col('schedule_id')))
    .withColumn('date',           random_date_col())
    .withColumn('start_hour',     wchoice(F.rand(), [str(h) for h in _start_h], _start_hw).cast('int'))
    .withColumn('_work_h',        wchoice(F.rand(), [str(h) for h in _work_h],  _work_hw).cast('int'))
    .withColumn('end_hour',       F.col('start_hour') + F.col('_work_h'))
    .withColumn('shift_type',     wchoice(F.rand(), ['day','morning','afternoon','night','day_off','holiday','sick_leave'], [0.40,0.15,0.15,0.02,0.15,0.08,0.05]))
    .withColumn('overtime_hours', wchoice(F.rand(), [0,0,0,0,0,1,2,3,4], [1/9]*9).cast('int'))
    .withColumn('attendance',     wchoice(F.rand(), ['present','absent_excused','absent_unexcused','late'], [0.88,0.08,0.02,0.02]))
    .drop('_work_h')
)
save_table(df_schedules, 'fact_employee_schedules')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation

# COMMAND ----------

print('=' * 60)
print('DATA GENERATION COMPLETE')
print('=' * 60)
print(f'SCALE_FACTOR={SCALE_FACTOR}  |  {DATE_START} → {DATE_END}')
print()
print('Target row counts:')
for name, n in [
    ('dim_locations', NUM_LOCATIONS), ('dim_employees', len(emp_rows)),
    ('dim_customers', NUM_CUSTOMERS), ('dim_vehicles',  NUM_VEHICLES),
    ('dim_products',  NUM_PRODUCTS),  ('dim_services',  NUM_SERVICES),
    ('dim_suppliers', NUM_SUPPLIERS),
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
    print(f'  {name:<35} {n:>15,}')

print()
for base_dir in [OUTPUT_DIR_DIM, OUTPUT_DIR_FACT]:
    try:
        items = dbutils.fs.ls(base_dir)
        print(f'{base_dir}: {len(items)} tables')
    except Exception as e:
        print(f'{base_dir}: {e}')