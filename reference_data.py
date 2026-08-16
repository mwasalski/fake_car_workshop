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
    'Toyota': ['Corolla', 'Yaris', 'RAV4', 'Camry', 'C-HR', 'Aygo', 'Hilux', 'Land Cruiser'],
    'Volkswagen': ['Golf', 'Passat', 'Polo', 'Tiguan', 'T-Roc', 'Arteon', 'Touran', 'Caddy'],
    'Skoda': ['Octavia', 'Fabia', 'Superb', 'Kodiaq', 'Karoq', 'Kamiq', 'Scala', 'Citigo'],
    'Ford': ['Focus', 'Fiesta', 'Mondeo', 'Kuga', 'Puma', 'EcoSport', 'Transit', 'Ranger'],
    'Opel': ['Astra', 'Corsa', 'Insignia', 'Mokka', 'Crossland', 'Grandland', 'Combo', 'Vivaro'],
    'BMW': ['Series 3', 'Series 5', 'X1', 'X3', 'Series 1', 'X5', 'Series 7', 'X6'],
    'Audi': ['A3', 'A4', 'A6', 'Q3', 'Q5', 'A1', 'Q7', 'TT'],
    'Mercedes': ['Class A', 'Class C', 'Class E', 'GLC', 'GLA', 'GLE', 'Class S', 'Sprinter'],
    'Renault': ['Clio', 'Megane', 'Captur', 'Kadjar', 'Scenic', 'Kangoo', 'Master', 'Trafic'],
    'Hyundai': ['i30', 'Tucson', 'i20', 'Kona', 'Santa Fe', 'i10', 'ix20', 'Ioniq'],
    'Kia': ['Ceed', 'Sportage', 'Rio', 'Stonic', 'Sorento', 'Picanto', 'XCeed', 'Niro'],
    'Fiat': ['500', 'Tipo', 'Panda', 'Punto', '500X', 'Ducato', 'Doblo', '500L'],
    'Peugeot': ['208', '308', '3008', '2008', '508', '5008', 'Partner', 'Rifter'],
    'Citroen': ['C3', 'C4', 'C5 Aircross', 'Berlingo', 'C3 Aircross', 'C1', 'Jumper', 'Jumpy'],
    'Dacia': ['Duster', 'Sandero', 'Logan', 'Dokker', 'Lodgy', 'Spring'],
    'Nissan': ['Qashqai', 'Juke', 'Micra', 'X-Trail', 'Navara', 'Leaf', 'Note'],
    'Honda': ['Civic', 'CR-V', 'Jazz', 'HR-V', 'Accord', 'e'],
    'Mazda': ['3', '6', 'CX-5', 'CX-3', 'CX-30', 'MX-5', '2'],
    'Volvo': ['XC60', 'XC40', 'V60', 'S60', 'XC90', 'V40', 'S90'],
    'Suzuki': ['Vitara', 'Swift', 'SX4 S-Cross', 'Ignis', 'Jimny', 'Baleno'],
    'Seat': ['Leon', 'Ibiza', 'Ateca', 'Arona', 'Alhambra', 'Toledo', 'Altea', 'Mii'],
    'Mitsubishi': ['Outlander', 'ASX', 'Lancer', 'Pajero', 'Space Star', 'L200', 'Colt', 'Eclipse Cross'],
    'Jeep': ['Renegade', 'Compass', 'Grand Cherokee', 'Cherokee', 'Wrangler', 'Avenger'],
    'Mini': ['Cooper', 'Countryman', 'Clubman', 'One', 'Paceman'],
    'Lexus': ['RX', 'NX', 'IS', 'CT', 'UX', 'ES', 'GS'],
    'Alfa Romeo': ['Giulietta', 'Giulia', 'Stelvio', 'MiTo', '159', 'Tonale'],
    'Land Rover': ['Range Rover Evoque', 'Discovery', 'Discovery Sport', 'Range Rover Sport', 'Defender', 'Freelander'],
    'Subaru': ['Forester', 'Outback', 'Impreza', 'XV', 'Legacy', 'BRZ'],
    'Chevrolet': ['Aveo', 'Cruze', 'Spark', 'Captiva', 'Orlando', 'Kalos'],
    'Tesla': ['Model 3', 'Model Y', 'Model S', 'Model X'],
    'Porsche': ['Cayenne', 'Macan', '911', 'Panamera', 'Boxster', 'Taycan'],
    'Cupra': ['Formentor', 'Leon', 'Ateca', 'Born', 'Terramar'],
    'SsangYong': ['Korando', 'Tivoli', 'Rexton', 'Actyon', 'Musso'],
    'Smart': ['Fortwo', 'Forfour', 'Roadster'],
    'MG': ['ZS', 'HS', 'MG4', 'MG5', 'Marvel R'],
}

# Brand popularity weights for Poland (used-car market, approximate)
MAKE_WEIGHTS = {
    'Volkswagen': 0.110, 'Opel': 0.085, 'Audi': 0.075, 'BMW': 0.070,
    'Ford': 0.070, 'Toyota': 0.068, 'Skoda': 0.065, 'Mercedes': 0.055,
    'Renault': 0.050, 'Peugeot': 0.040, 'Citroen': 0.030, 'Hyundai': 0.030,
    'Kia': 0.028, 'Fiat': 0.028, 'Seat': 0.025, 'Nissan': 0.022,
    'Mazda': 0.020, 'Dacia': 0.020, 'Volvo': 0.018, 'Honda': 0.015,
    'Mitsubishi': 0.010, 'Suzuki': 0.010, 'Mini': 0.008, 'Jeep': 0.007,
    'Lexus': 0.006, 'Alfa Romeo': 0.006, 'Land Rover': 0.005, 'Subaru': 0.004,
    'Chevrolet': 0.004, 'Porsche': 0.003, 'Tesla': 0.003, 'Cupra': 0.003,
    'Smart': 0.003, 'SsangYong': 0.002, 'MG': 0.002,
}


FUEL_TYPES = ['petrol', 'diesel', 'LPG', 'hybrid', 'electric']
FUEL_WEIGHTS = [0.35, 0.30, 0.15, 0.15, 0.05]

COLORS = ['white', 'black', 'silver', 'grey', 'red', 'blue',
          'navy', 'green', 'brown', 'beige', 'gold', 'maroon']
PRODUCT_CATEGORIES = {
    'Oils and Fluids': [
        'Engine oil 5W-30', 'Engine oil 5W-40', 'Engine oil 10W-40',
        'Engine oil 0W-20', 'Brake fluid DOT4', 'Coolant G12',
        'Summer windscreen wash', 'Winter windscreen wash',
        'Gearbox oil', 'Power steering fluid', 'AdBlue fluid 10L',
        'Engine oil 5W-20', 'Engine oil 15W-40', 'Engine oil 0W-30',
        'Brake fluid DOT3', 'Brake fluid DOT5.1', 'Coolant G11', 'Coolant G13',
        'Concentrated screenwash', 'Manual gearbox oil 75W-90',
        'Automatic transmission fluid ATF', 'Differential oil',
    ],
    'Filters': [
        'Oil filter', 'Air filter', 'Cabin filter', 'Fuel filter',
        'Active carbon cabin filter', 'DPF filter', 'GPF filter',
        'Hydraulic filter', 'Transmission filter', 'HEPA cabin filter',
        'Sport air filter', 'Diesel pre-filter', 'Crankcase breather filter',
        'Fuel filter with water separator',
    ],
    'Brake Pads and Discs': [
        'Brake pads front', 'Brake pads rear',
        'Brake discs front', 'Brake discs rear',
        'Brake shoes', 'Brake drums',
        'Brake caliper front', 'Brake caliper rear', 'Brake hose',
        'Brake wear sensor', 'Handbrake cable', 'Brake master cylinder',
    ],
    'Tyres': [
        'Summer tyre 205/55 R16', 'Summer tyre 195/65 R15',
        'Summer tyre 225/45 R17', 'Winter tyre 205/55 R16',
        'Winter tyre 195/65 R15', 'Winter tyre 225/45 R17',
        'All-season tyre 205/55 R16', 'All-season tyre 195/65 R15',
        'Summer tyre 215/60 R16', 'Summer tyre 235/55 R18',
        'Winter tyre 215/60 R16', 'Winter tyre 235/55 R18',
        'All-season tyre 225/45 R17', 'All-season tyre 215/60 R16',
        'Steel wheel 16"', 'Alloy wheel 17"',
    ],
    'Batteries': [
        'Battery 60Ah', 'Battery 70Ah', 'Battery 74Ah',
        'Battery 80Ah', 'Battery 100Ah', 'AGM Battery 70Ah',
        'Battery 44Ah', 'Battery 55Ah', 'Battery 90Ah',
        'AGM Battery 80Ah', 'AGM Battery 95Ah', 'EFB Battery 70Ah',
    ],
    'Lighting': [
        'H7 bulb', 'H4 bulb', 'H1 bulb', 'LED H7 bulb',
        'LED H4 bulb', 'W5W bulb', 'P21W bulb',
        'D1S xenon bulb', 'D2S xenon bulb',
        'H11 bulb', 'H15 bulb', 'HB3 bulb', 'HB4 bulb', 'LED H11 bulb',
        'PY21W bulb', 'C5W bulb', 'D3S xenon bulb',
        'LED daytime running light set',
    ],
    'Wipers': [
        'Front left wiper blade', 'Front right wiper blade',
        'Rear wiper blade', 'Front wiper blade set',
        'Flat wiper blade 600mm', 'Flat wiper blade 450mm',
        'Front wiper arm', 'Wiper rubber refill',
    ],
    'Suspension System': [
        'Front shock absorber', 'Rear shock absorber', 'Suspension spring',
        'Lower control arm', 'Stabiliser link', 'Control arm bushing',
        'Tie rod end', 'Tie rod',
        'Front wheel bearing', 'Rear wheel bearing', 'Strut mount',
        'Rear coil spring', 'Anti-roll bar bush', 'Ball joint',
        'Rear axle beam bushing', 'Suspension strut assembly',
    ],
    'Timing System': [
        'Timing belt', 'Timing kit with water pump',
        'Multi-V belt', 'Timing belt tensioner',
        'Timing chain', 'Timing chain kit',
        'Timing chain tensioner', 'Camshaft sprocket', 'Crankshaft pulley',
        'Water pump', 'Multi-V belt tensioner', 'Idler pulley',
    ],
    'Exhaust System': [
        'Rear muffler', 'Middle muffler', 'Catalytic converter',
        'Exhaust pipe', 'DPF particulate filter',
        'Lambda sensor', 'Exhaust gasket',
        'Front exhaust pipe with catalyst', 'EGR valve', 'Exhaust clamp',
        'Exhaust mounting rubber', 'Turbocharger', 'SCR NOx sensor',
        'Exhaust manifold',
    ],
    'Electrical System': [
        'Alternator', 'Starter motor', 'Ignition coil',
        'Spark plug', 'Glow plug', 'ABS sensor',
        'Temperature sensor', 'Oil pressure sensor',
        'Ignition cable set', 'Crankshaft position sensor',
        'Camshaft position sensor', 'MAF sensor', 'MAP sensor',
        'Knock sensor', 'Alternator regulator', 'Fuse box relay',
    ],
    'Clutch': [
        'Clutch kit', 'Clutch disc', 'Clutch pressure plate',
        'Clutch release bearing', 'Dual mass flywheel',
        'Clutch master cylinder', 'Clutch slave cylinder', 'Clutch cable',
        'Single mass flywheel', 'Clutch kit with dual mass flywheel',
    ],
    'Car Care Products': [
        'Car shampoo', 'Paint wax', 'Wheel cleaner',
        'De-icer', 'Air freshener', 'Polishing compound',
        'Upholstery cleaner', 'Seal silicone',
        'Plastic restorer', 'Anti-corrosion spray',
        'Ceramic coating', 'Glass cleaner', 'Tar remover', 'Tyre shine',
        'Microfibre cloth set', 'Washing sponge', 'Chamois leather',
        'Insect remover', 'Leather conditioner', 'Engine bay cleaner',
    ],
    'Accessories': [
        'Rubber floor mat set', 'Velour floor mat set',
        'Seat covers', 'Boot organiser', 'First aid kit',
        'Warning triangle', 'Car fire extinguisher', 'Jump leads',
        'Car compass', 'USB car charger',
        'Phone holder', 'Reversing camera', 'Parking sensors',
        'Roof rack', 'Roof box', 'Tow bar',
        'Snow chains', 'Ice scraper', 'Snow brush', 'Windscreen cover',
        'Sunshade', 'Car vacuum cleaner', 'Tyre inflator compressor',
        'Dashcam', 'GPS navigation', 'Bluetooth car kit',
        'Child car seat', 'Booster seat', 'Dog car barrier',
        'Boot liner', 'Mudflap set', 'Tow rope',
    ],
    'Tools': [
        'Wheel wrench', 'Hydraulic jack', 'Socket wrench set',
        'Torque wrench', 'Tyre repair kit',
        'Oil filter wrench', 'OBD2 diagnostic scanner', 'Battery charger',
        'Multimeter', 'Wheel chock set',
    ],
}

SERVICE_CATALOGUE = [
    # (name, category, min_price_net, max_price_net, estimated_time_min)
    ('Oil and filter change', 'Periodic Service', 80, 150, 30),
    ('Periodic service', 'Periodic Service', 150, 350, 60),
    ('Air filter replacement', 'Periodic Service', 30, 60, 15),
    ('Cabin filter replacement', 'Periodic Service', 30, 60, 15),
    ('Brake fluid replacement', 'Periodic Service', 80, 150, 30),
    ('Coolant replacement', 'Periodic Service', 100, 200, 45),
    ('Spark plug replacement', 'Periodic Service', 60, 150, 30),
    ('Glow plug replacement', 'Periodic Service', 100, 300, 60),
    ('Fuel filter replacement', 'Periodic Service', 60, 150, 30),
    ('Gearbox oil replacement', 'Periodic Service', 150, 400, 60),
    ('Automatic transmission service', 'Periodic Service', 400, 1200, 120),
    ('Differential oil replacement', 'Periodic Service', 100, 250, 45),
    ('Power steering fluid replacement', 'Periodic Service', 80, 180, 45),
    ('AdBlue top-up', 'Periodic Service', 40, 100, 15),
    ('Windscreen wash top-up', 'Periodic Service', 20, 40, 10),
    ('Pre-winter check', 'Periodic Service', 60, 150, 45),
    ('Brake pads replacement front', 'Brakes', 100, 200, 45),
    ('Brake pads replacement rear', 'Brakes', 80, 180, 45),
    ('Brake discs and pads replacement front', 'Brakes', 200, 400, 60),
    ('Brake discs and pads replacement rear', 'Brakes', 180, 350, 60),
    ('Brake shoes replacement', 'Brakes', 100, 200, 60),
    ('Brake caliper replacement', 'Brakes', 150, 400, 90),
    ('Brake hose replacement', 'Brakes', 80, 200, 60),
    ('Handbrake adjustment', 'Brakes', 40, 100, 30),
    ('Brake system bleeding', 'Brakes', 80, 160, 45),
    ('Brake master cylinder replacement', 'Brakes', 200, 500, 120),
    ('Tyre change (4 pcs)', 'Tyres', 80, 160, 45),
    ('Wheel balancing (4 pcs)', 'Tyres', 40, 80, 30),
    ('Tyre storage (season)', 'Tyres', 60, 120, 15),
    ('Tyre repair', 'Tyres', 20, 50, 20),
    ('Wheel alignment', 'Tyres', 100, 200, 45),
    ('Seasonal wheel swap (4 pcs)', 'Tyres', 50, 100, 30),
    ('TPMS sensor replacement', 'Tyres', 100, 300, 45),
    ('Wheel straightening', 'Tyres', 80, 200, 60),
    ('Runflat tyre fitting (4 pcs)', 'Tyres', 120, 240, 60),
    ('Front shock absorber replacement', 'Suspension', 200, 500, 120),
    ('Rear shock absorber replacement', 'Suspension', 150, 400, 90),
    ('Control arm replacement', 'Suspension', 150, 350, 90),
    ('Stabiliser link replacement', 'Suspension', 50, 120, 30),
    ('Tie rod end replacement', 'Suspension', 80, 180, 45),
    ('Wheel bearing replacement', 'Suspension', 200, 500, 120),
    ('Suspension spring replacement', 'Suspension', 200, 500, 120),
    ('Strut mount replacement', 'Suspension', 150, 350, 90),
    ('Suspension inspection', 'Suspension', 50, 120, 30),
    ('Timing belt replacement', 'Timing', 400, 1200, 240),
    ('Timing kit with water pump replacement', 'Timing', 600, 1800, 300),
    ('Multi-V belt replacement', 'Timing', 80, 200, 45),
    ('Timing chain replacement', 'Timing', 800, 3000, 480),
    ('Water pump replacement', 'Timing', 300, 800, 180),
    ('Multi-V belt tensioner replacement', 'Timing', 150, 400, 90),
    ('Clutch replacement', 'Clutch', 500, 1500, 360),
    ('Dual mass flywheel replacement', 'Clutch', 800, 2500, 420),
    ('Clutch slave cylinder replacement', 'Clutch', 150, 400, 90),
    ('Clutch adjustment', 'Clutch', 60, 150, 45),
    ('Starter motor replacement', 'Electrical System', 200, 500, 90),
    ('Alternator replacement', 'Electrical System', 250, 600, 90),
    ('Ignition coil replacement', 'Electrical System', 100, 300, 45),
    ('ABS sensor replacement', 'Electrical System', 100, 250, 45),
    ('Lambda sensor replacement', 'Electrical System', 150, 400, 60),
    ('Wiring harness repair', 'Electrical System', 100, 600, 120),
    ('Computer diagnostics', 'Diagnostics', 50, 150, 30),
    ('Error code clearing', 'Diagnostics', 30, 80, 15),
    ('Chassis diagnostics', 'Diagnostics', 80, 200, 45),
    ('Compression test', 'Diagnostics', 100, 250, 60),
    ('Pre-purchase inspection', 'Diagnostics', 150, 400, 90),
    ('Air conditioning check', 'Air Conditioning', 50, 100, 30),
    ('Air conditioning service', 'Air Conditioning', 150, 350, 60),
    ('Interior ozone treatment', 'Air Conditioning', 50, 100, 30),
    ('Air conditioning leak detection', 'Air Conditioning', 100, 250, 60),
    ('Air conditioning compressor replacement', 'Air Conditioning', 600, 2000, 180),
    ('Muffler replacement', 'Exhaust System', 150, 400, 60),
    ('Catalytic converter replacement', 'Exhaust System', 500, 2000, 120),
    ('Exhaust welding', 'Exhaust System', 50, 150, 30),
    ('DPF cleaning', 'Exhaust System', 400, 1200, 180),
    ('EGR valve replacement', 'Exhaust System', 300, 900, 120),
    ('Turbocharger replacement', 'Exhaust System', 1000, 4000, 300),
    ('Battery replacement', 'Electrics', 30, 60, 15),
    ('Bulb replacement', 'Electrics', 20, 80, 15),
    ('Headlight adjustment', 'Electrics', 40, 100, 20),
    ('Headlight polishing', 'Electrics', 100, 250, 60),
    ('Panel painting', 'Bodywork', 300, 1500, 480),
    ('Bodywork and paint repair', 'Bodywork', 500, 5000, 960),
    ('Paint polishing', 'Bodywork', 200, 600, 240),
    ('PDR dent removal', 'Bodywork', 100, 500, 120),
    ('Windscreen replacement', 'Bodywork', 400, 1500, 120),
    ('Windscreen chip repair', 'Bodywork', 80, 200, 45),
    ('Scratch removal', 'Bodywork', 150, 500, 120),
    ('Underbody sealing', 'Bodywork', 300, 900, 180),
    ('Technical inspection', 'Inspection', 99, 99, 30),
    ('Technical inspection + emissions test', 'Inspection', 162, 162, 45),
    ('Technical inspection - LPG', 'Inspection', 162, 162, 45),
    ('Engine cleaning', 'Other', 80, 200, 60),
    ('Chassis anti-corrosion treatment', 'Other', 200, 600, 120),
    ('Interior deep cleaning', 'Other', 150, 500, 180),
    ('Car wash and wax', 'Other', 50, 150, 60),
    ('Vehicle collection and return', 'Other', 50, 150, 60),
]

PAYMENT_METHODS = ['cash', 'card', 'bank_transfer', 'BLIK', 'leasing', 'instalments']
PAYMENT_WEIGHTS = [0.15, 0.40, 0.20, 0.15, 0.05, 0.05]

WORK_ORDER_STATUSES = ['new', 'in_progress', 'waiting_for_parts', 'completed', 'cancelled']
STATUS_WEIGHTS = [0.02, 0.03, 0.01, 0.92, 0.02]

LOCATION_TYPES = ['workshop', 'shop', 'workshop_and_shop']
LOCATION_TYPE_WEIGHTS = [0.30, 0.20, 0.50]

POSITIONS = {
    'workshop': ['mechanic', 'senior_mechanic', 'auto_electrician', 'panel_beater', 'painter', 'diagnostician'],
    'shop': ['sales_assistant', 'senior_sales_assistant', 'cashier', 'warehouse_operative'],
    'management': ['branch_manager', 'deputy_manager', 'accountant'],
}

# ============================================================
# PRODUCT MANUFACTURERS & PRICING RULES
# ============================================================

MANUFACTURERS = [
    'Bosch', 'Continental', 'Valeo', 'Hella', 'Mann', 'Mahle', 'NGK',
    'Brembo', 'TRW', 'KYB', 'Monroe', 'Sachs', 'LuK', 'Gates',
    'SKF', 'Dayco', 'Castrol', 'Mobil', 'Shell', 'Total', 'Motul',
    'Liqui Moly', 'K2', 'Meguiars', 'Sonax', 'Goodyear', 'Michelin',
    'Bridgestone', 'Pirelli', 'Varta', 'Exide', 'Banner',
]

# (keyword matched against lowercase product name, price_min, price_max)
PRODUCT_PRICE_RULES = [
    ('tyre', 180, 600),
    ('battery', 250, 800),
    ('clutch kit', 400, 2000),
    ('dual mass flywheel', 400, 2000),
    ('shock absorber', 100, 400),
    ('filter', 15, 80),
    ('brake pads', 60, 300),
    ('brake discs', 60, 300),
    ('oil', 30, 180),
    ('bulb', 8, 120),
]
DEFAULT_PRICE_RANGE = (5, 800)

# ============================================================
# FACT TABLE REFERENCE LISTS
# ============================================================

WORK_ORDER_NOTES = [
    '', '', 'Knocking noise when braking', 'Engine losing power', 'Oil leak',
    'Seasonal tyre change', 'Periodic service', 'Air conditioning not cooling',
    'Engine warning light', 'Suspension noise', 'Brake pad replacement',
    'Preparation for inspection', 'Oil change', 'Starter motor problem',
    'Steering wheel vibration', 'Spark plug replacement',
]

APPOINTMENT_STATUSES = ['confirmed', 'completed', 'cancelled', 'no_show']
APPOINTMENT_STATUS_WEIGHTS = [0.10, 0.75, 0.10, 0.05]

BOOKING_CHANNELS = ['phone', 'online', 'in_person', 'email']
BOOKING_CHANNEL_WEIGHTS = [0.35, 0.40, 0.15, 0.10]

APPOINTMENT_NOTES = [
    '', '', '', 'Please call before', 'Courtesy car needed',
    'Prefer morning', 'Urgent', 'Previously arranged',
]

INVOICE_DOCUMENT_TYPES = ['vat_invoice', 'receipt', 'credit_note']
INVOICE_DOCUMENT_TYPE_WEIGHTS = [0.35, 0.60, 0.05]

INVOICE_STATUSES = ['paid', 'pending', 'overdue', 'cancelled']
INVOICE_STATUS_WEIGHTS = [0.80, 0.10, 0.07, 0.03]

PAYMENT_STATUSES = ['completed', 'pending', 'rejected', 'refund']
PAYMENT_STATUS_WEIGHTS = [0.90, 0.05, 0.03, 0.02]

PO_STATUSES = ['placed', 'in_progress', 'delivered', 'partially_delivered', 'cancelled']
PO_STATUS_WEIGHTS = [0.03, 0.05, 0.85, 0.05, 0.02]

MOVEMENT_TYPES = ['receipt', 'issue_sales', 'issue_workshop', 'return', 'correction', 'stocktake']
MOVEMENT_TYPE_WEIGHTS = [0.25, 0.35, 0.25, 0.05, 0.05, 0.05]

SOURCE_DOCUMENTS = ['GR', 'GI', 'RM', 'RT', 'COR', 'INV']

INVENTORY_NOTES = [
    '', '', '', 'Regular delivery', 'Special order',
    'Customer return', 'Stock correction',
]

FEEDBACK_COMMENTS = [
    'Very professional service', 'Quick turnaround', 'Highly recommended!',
    'A bit overpriced', 'Long waiting time', 'Great communication',
    'Expert repair', 'Car ready ahead of schedule', 'Friendly staff',
    'Could be cheaper', 'Will definitely come back', 'Solid work',
    'Fair prices', 'Problem returned after a month', 'No complaints',
    'Excellent!', 'Average', 'Needs improvement', 'OK', '',
]

FEEDBACK_CATEGORIES = ['service_quality', 'repair_quality', 'turnaround_time', 'price', 'cleanliness', 'overall']
FEEDBACK_CATEGORY_WEIGHTS = [0.20, 0.25, 0.15, 0.15, 0.10, 0.15]

FEEDBACK_CHANNELS = ['google', 'online_form', 'email', 'phone']
FEEDBACK_CHANNEL_WEIGHTS = [0.40, 0.30, 0.20, 0.10]

RATING_WEIGHTS = [0.03, 0.05, 0.12, 0.30, 0.50]  # ratings 1..5

LOYALTY_EVENT_TYPES = ['points_earned', 'points_redeemed', 'bonus', 'expiry']
LOYALTY_EVENT_WEIGHTS = [0.60, 0.20, 0.10, 0.10]

LOYALTY_POINTS = [-500, -200, -100, 10, 20, 50, 100, 200, 500]
LOYALTY_POINT_WEIGHTS = [0.05, 0.07, 0.08, 0.20, 0.25, 0.15, 0.10, 0.05, 0.05]

LOYALTY_DESCRIPTIONS = [
    'Shop purchase', 'Workshop service', 'Welcome bonus',
    'Birthday bonus', 'Redeemed for 10% discount', 'Redeemed for 20% discount',
    'Redeemed for free service', 'Points expired', 'Referral bonus',
]

LOYALTY_TIERS = ['standard', 'silver', 'gold', 'platinum']
LOYALTY_TIER_WEIGHTS = [0.50, 0.30, 0.15, 0.05]

SHIFT_TYPES = ['day', 'morning', 'afternoon', 'night', 'day_off', 'holiday', 'sick_leave']
SHIFT_TYPE_WEIGHTS = [0.40, 0.15, 0.15, 0.02, 0.15, 0.08, 0.05]

SHIFT_START_HOURS = [6, 7, 8, 9, 10, 12, 14]
SHIFT_START_WEIGHTS = [0.05, 0.25, 0.30, 0.15, 0.05, 0.10, 0.10]

SHIFT_LENGTHS = [4, 6, 8, 10, 12]
SHIFT_LENGTH_WEIGHTS = [0.10, 0.10, 0.60, 0.15, 0.05]

ATTENDANCE_TYPES = ['present', 'absent_excused', 'absent_unexcused', 'late']
ATTENDANCE_WEIGHTS = [0.88, 0.08, 0.02, 0.02]