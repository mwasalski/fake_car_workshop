# ============================================================
# SŁOWNIKI DANYCH REFERENCYJNYCH
# ============================================================

#STATES
WOJEWODZTWA = [
    'dolnośląskie', 'kujawsko-pomorskie', 'lubelskie', 'lubuskie',
    'łódzkie', 'małopolskie', 'mazowieckie', 'opolskie',
    'podkarpackie', 'podlaskie', 'pomorskie', 'śląskie',
    'świętokrzyskie', 'warmińsko-mazurskie', 'wielkopolskie', 'zachodniopomorskie'
]

#CITIES
MIASTA = [
    ('Warszawa', 'mazowieckie', 52.2297, 21.0122),
    ('Kraków', 'małopolskie', 50.0647, 19.9450),
    ('Łódź', 'łódzkie', 51.7592, 19.4560),
    ('Wrocław', 'dolnośląskie', 51.1079, 17.0385),
    ('Poznań', 'wielkopolskie', 52.4064, 16.9252),
    ('Gdańsk', 'pomorskie', 54.3520, 18.6466),
    ('Szczecin', 'zachodniopomorskie', 53.4285, 14.5528),
    ('Bydgoszcz', 'kujawsko-pomorskie', 53.1235, 18.0084),
    ('Lublin', 'lubelskie', 51.2465, 22.5684),
    ('Białystok', 'podlaskie', 53.1325, 23.1688),
    ('Katowice', 'śląskie', 50.2649, 19.0238),
    ('Gdynia', 'pomorskie', 54.5189, 18.5305),
    ('Częstochowa', 'śląskie', 50.8118, 19.1203),
    ('Radom', 'mazowieckie', 51.4027, 21.1471),
    ('Sosnowiec', 'śląskie', 50.2863, 19.1041),
    ('Toruń', 'kujawsko-pomorskie', 53.0138, 18.5984),
    ('Kielce', 'świętokrzyskie', 50.8661, 20.6286),
    ('Rzeszów', 'podkarpackie', 50.0412, 21.9991),
    ('Gliwice', 'śląskie', 50.2945, 18.6714),
    ('Zabrze', 'śląskie', 50.3249, 18.7857),
    ('Olsztyn', 'warmińsko-mazurskie', 53.7784, 20.4801),
    ('Bielsko-Biała', 'śląskie', 49.8224, 19.0586),
    ('Bytom', 'śląskie', 50.3483, 18.9157),
    ('Zielona Góra', 'lubuskie', 51.9356, 15.5062),
    ('Rybnik', 'śląskie', 50.1022, 18.5463),
    ('Ruda Śląska', 'śląskie', 50.2558, 18.8556),
    ('Opole', 'opolskie', 50.6751, 17.9213),
    ('Tychy', 'śląskie', 50.1357, 18.9936),
    ('Gorzów Wielkopolski', 'lubuskie', 52.7325, 15.2369),
    ('Elbląg', 'warmińsko-mazurskie', 54.1522, 19.4088),
    ('Płock', 'mazowieckie', 52.5463, 19.7065),
    ('Dąbrowa Górnicza', 'śląskie', 50.3217, 19.1880),
    ('Wałbrzych', 'dolnośląskie', 50.7714, 16.2843),
    ('Włocławek', 'kujawsko-pomorskie', 52.6483, 19.0677),
    ('Tarnów', 'małopolskie', 50.0121, 20.9858),
    ('Chorzów', 'śląskie', 50.2975, 18.9545),
    ('Koszalin', 'zachodniopomorskie', 54.1943, 16.1715),
    ('Kalisz', 'wielkopolskie', 51.7611, 18.0909),
    ('Legnica', 'dolnośląskie', 51.2070, 16.1619),
    ('Grudziądz', 'kujawsko-pomorskie', 53.4837, 18.7536),
    ('Jaworzno', 'śląskie', 50.2040, 19.2747),
    ('Słupsk', 'pomorskie', 54.4641, 17.0285),
    ('Jastrzębie-Zdrój', 'śląskie', 49.9477, 18.5963),
    ('Nowy Sącz', 'małopolskie', 49.6249, 20.6915),
    ('Jelenia Góra', 'dolnośląskie', 50.9044, 15.7197),
    ('Siedlce', 'mazowieckie', 52.1676, 22.2903),
    ('Mysłowice', 'śląskie', 50.2083, 19.1666),
    ('Konin', 'wielkopolskie', 52.2230, 18.2511),
    ('Piła', 'wielkopolskie', 53.1510, 16.7382),
    ('Piotrków Trybunalski', 'łódzkie', 51.4053, 19.7031),
    ('Inowrocław', 'kujawsko-pomorskie', 52.7936, 18.2614),
    ('Lubin', 'dolnośląskie', 51.4010, 16.2015),
    ('Ostrów Wielkopolski', 'wielkopolskie', 51.6550, 17.8068),
    ('Suwałki', 'podlaskie', 54.1118, 22.9308),
    ('Stargard', 'zachodniopomorskie', 53.3364, 15.0502),
    ('Gniezno', 'wielkopolskie', 52.5348, 17.5827),
    ('Ostrowiec Świętokrzyski', 'świętokrzyskie', 50.9295, 21.3856),
    ('Siemianowice Śląskie', 'śląskie', 50.3264, 19.0296),
    ('Głogów', 'dolnośląskie', 51.6634, 16.0845),
    ('Pabianice', 'łódzkie', 51.6649, 19.3548),
    ('Leszno', 'wielkopolskie', 51.8425, 16.5749),
    ('Żory', 'śląskie', 50.0455, 18.7005),
    ('Pruszków', 'mazowieckie', 52.1707, 20.8120),
    ('Stalowa Wola', 'podkarpackie', 50.5828, 22.0531),
    ('Zamość', 'lubelskie', 50.7230, 23.2519),
    ('Łomża', 'podlaskie', 53.1784, 22.0593),
    ('Mielec', 'podkarpackie', 50.2874, 21.4260),
    ('Tczew', 'pomorskie', 54.0927, 18.7955),
    ('Chełm', 'lubelskie', 51.1431, 23.4716),
    ('Przemyśl', 'podkarpackie', 49.7838, 22.7678),
    ('Starachowice', 'świętokrzyskie', 51.0378, 21.0714),
    ('Wejherowo', 'pomorskie', 54.6059, 18.2354),
    ('Puławy', 'lubelskie', 51.4166, 21.9686),
    ('Skierniewice', 'łódzkie', 51.9542, 20.1576),
    ('Skarżysko-Kamienna', 'świętokrzyskie', 51.1141, 20.8597),
    ('Tarnobrzeg', 'podkarpackie', 50.5731, 21.6792),
    ('Radomsko', 'łódzkie', 51.0671, 19.4462),
    ('Kędzierzyn-Koźle', 'opolskie', 50.3494, 18.2074),
    ('Biała Podlaska', 'lubelskie', 52.0326, 23.1166),
    ('Oświęcim', 'małopolskie', 50.0343, 19.2098),
    ('Sandomierz', 'świętokrzyskie', 50.6827, 21.7489),
    ('Busko-Zdrój', 'świętokrzyskie', 50.4710, 20.7192),
    ('Nowa Sól', 'lubuskie', 51.8063, 15.7146),
    ('Nysa', 'opolskie', 50.4743, 17.3346),
    ('Otwock', 'mazowieckie', 52.1054, 21.2614),
    ('Szczytno', 'warmińsko-mazurskie', 53.5630, 20.9868),
    ('Kutno', 'łódzkie', 52.2318, 19.3569),
    ('Sanok', 'podkarpackie', 49.5566, 22.2059),
    ('Świnoujście', 'zachodniopomorskie', 53.9101, 14.2474),
    ('Świdnica', 'dolnośląskie', 50.8463, 16.4872),
    ('Chojnice', 'pomorskie', 53.6953, 17.5551),
    ('Mińsk Mazowiecki', 'mazowieckie', 52.1790, 21.5617),
    ('Żyrardów', 'mazowieckie', 52.0491, 20.4467),
    ('Wołomin', 'mazowieckie', 52.3461, 21.2405),
    ('Nowy Targ', 'małopolskie', 49.4782, 20.0323),
    ('Giżycko', 'warmińsko-mazurskie', 54.0380, 21.7647),
    ('Brodnica', 'kujawsko-pomorskie', 53.2600, 19.3954),
    ('Bolesławiec', 'dolnośląskie', 51.2622, 15.5694),
    ('Świecie', 'kujawsko-pomorskie', 53.4100, 18.4316),
]

#CAR MANUFACTURERS
MARKI_SAMOCHODOW = {
    'Toyota': ['Corolla', 'Yaris', 'RAV4', 'Camry', 'C-HR', 'Aygo', 'Hilux', 'Land Cruiser'],
    'Volkswagen': ['Golf', 'Passat', 'Polo', 'Tiguan', 'T-Roc', 'Arteon', 'Touran', 'Caddy'],
    'Skoda': ['Octavia', 'Fabia', 'Superb', 'Kodiaq', 'Karoq', 'Kamiq', 'Scala', 'Citigo'],
    'Ford': ['Focus', 'Fiesta', 'Mondeo', 'Kuga', 'Puma', 'EcoSport', 'Transit', 'Ranger'],
    'Opel': ['Astra', 'Corsa', 'Insignia', 'Mokka', 'Crossland', 'Grandland', 'Combo', 'Vivaro'],
    'BMW': ['Seria 3', 'Seria 5', 'X1', 'X3', 'Seria 1', 'X5', 'Seria 7', 'X6'],
    'Audi': ['A3', 'A4', 'A6', 'Q3', 'Q5', 'A1', 'Q7', 'TT'],
    'Mercedes': ['Klasa A', 'Klasa C', 'Klasa E', 'GLC', 'GLA', 'GLE', 'Klasa S', 'Sprinter'],
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
}

# Wagi popularności marek w Polsce (sumują się do ~1)
MARKA_WAGI = {
    'Toyota': 0.12, 'Volkswagen': 0.11, 'Skoda': 0.10, 'Ford': 0.08,
    'Opel': 0.08, 'BMW': 0.05, 'Audi': 0.05, 'Mercedes': 0.04,
    'Renault': 0.06, 'Hyundai': 0.06, 'Kia': 0.06, 'Fiat': 0.04,
    'Peugeot': 0.04, 'Citroen': 0.03, 'Dacia': 0.03, 'Nissan': 0.02,
    'Honda': 0.02, 'Mazda': 0.02, 'Volvo': 0.02, 'Suzuki': 0.02,
}

TYPY_PALIWA = ['benzyna', 'diesel', 'LPG', 'hybryda', 'elektryczny']
PALIWO_WAGI = [0.35, 0.30, 0.15, 0.15, 0.05]

KOLORY = ['biały', 'czarny', 'srebrny', 'szary', 'czerwony', 'niebieski',
           'granatowy', 'zielony', 'brązowy', 'beżowy', 'złoty', 'bordowy']

KATEGORIE_PRODUKTOW = {
    'Oleje i płyny': [
        'Olej silnikowy 5W-30', 'Olej silnikowy 5W-40', 'Olej silnikowy 10W-40',
        'Olej silnikowy 0W-20', 'Płyn hamulcowy DOT4', 'Płyn chłodniczy G12',
        'Płyn do spryskiwaczy letni', 'Płyn do spryskiwaczy zimowy',
        'Olej przekładniowy', 'Płyn do wspomagania', 'Płyn AdBlue 10L',
    ],
    'Filtry': [
        'Filtr oleju', 'Filtr powietrza', 'Filtr kabinowy', 'Filtr paliwa',
        'Filtr kabinowy z węglem aktywnym', 'Filtr DPF', 'Filtr GPF',
    ],
    'Klocki i tarcze hamulcowe': [
        'Klocki hamulcowe przód', 'Klocki hamulcowe tył',
        'Tarcze hamulcowe przód', 'Tarcze hamulcowe tył',
        'Szczęki hamulcowe', 'Bębny hamulcowe',
    ],
    'Opony': [
        'Opona letnia 205/55 R16', 'Opona letnia 195/65 R15',
        'Opona letnia 225/45 R17', 'Opona zimowa 205/55 R16',
        'Opona zimowa 195/65 R15', 'Opona zimowa 225/45 R17',
        'Opona całoroczna 205/55 R16', 'Opona całoroczna 195/65 R15',
    ],
    'Akumulatory': [
        'Akumulator 60Ah', 'Akumulator 70Ah', 'Akumulator 74Ah',
        'Akumulator 80Ah', 'Akumulator 100Ah', 'Akumulator AGM 70Ah',
    ],
    'Oświetlenie': [
        'Żarówka H7', 'Żarówka H4', 'Żarówka H1', 'Żarówka LED H7',
        'Żarówka LED H4', 'Żarówka W5W', 'Żarówka P21W',
        'Żarówka ksenonowa D1S', 'Żarówka ksenonowa D2S',
    ],
    'Wycieraczki': [
        'Wycieraczka przednia lewa', 'Wycieraczka przednia prawa',
        'Wycieraczka tylna', 'Komplet wycieraczek przednich',
    ],
    'Układ zawieszenia': [
        'Amortyzator przedni', 'Amortyzator tylny', 'Sprężyna zawieszenia',
        'Wahacz dolny', 'Łącznik stabilizatora', 'Tuleja wahacza',
        'Końcówka drążka kierowniczego', 'Drążek kierowniczy',
    ],
    'Układ rozrządu': [
        'Pasek rozrządu', 'Zestaw rozrządu z pompą wody',
        'Pasek klinowy wielorowkowy', 'Napinacz paska rozrządu',
        'Łańcuch rozrządu', 'Zestaw łańcucha rozrządu',
    ],
    'Układ wydechowy': [
        'Tłumik końcowy', 'Tłumik środkowy', 'Katalizator',
        'Rura wydechowa', 'Filtr cząstek stałych DPF',
        'Sonda lambda', 'Uszczelka wydechu',
    ],
    'Układ elektryczny': [
        'Alternator', 'Rozrusznik', 'Cewka zapłonowa',
        'Świeca zapłonowa', 'Świeca żarowa', 'Czujnik ABS',
        'Czujnik temperatury', 'Czujnik ciśnienia oleju',
    ],
    'Sprzęgło': [
        'Komplet sprzęgła', 'Tarcza sprzęgła', 'Docisk sprzęgła',
        'Łożysko oporowe sprzęgła', 'Koło dwumasowe',
    ],
    'Chemia samochodowa': [
        'Szampon samochodowy', 'Wosk do lakieru', 'Środek do felg',
        'Odmrażacz do szyb', 'Odświeżacz powietrza', 'Pasta polerska',
        'Środek do czyszczenia tapicerki', 'Silikon do uszczelek',
        'Środek do plastików', 'Preparat antykorozyjny',
    ],
    'Akcesoria': [
        'Dywaniki gumowe komplet', 'Dywaniki welurowe komplet',
        'Pokrowce na fotele', 'Organizer bagażnika', 'Apteczka samochodowa',
        'Trójkąt ostrzegawczy', 'Gaśnica samochodowa', 'Kable rozruchowe',
        'Kompas samochodowy', 'Ładowarka USB do samochodu',
        'Uchwyt na telefon', 'Kamera cofania', 'Czujniki parkowania',
        'Bagażnik dachowy', 'Box dachowy', 'Hak holowniczy',
    ],
    'Narzędzia': [
        'Klucz do kół', 'Podnośnik hydrauliczny', 'Komplet kluczy nasadowych',
        'Klucz dynamometryczny', 'Zestaw naprawczy opon',
    ],
}

KATALOG_USLUG = [
    # (nazwa, kategoria, min_cena_netto, max_cena_netto, czas_min)
    ('Wymiana oleju i filtra', 'Serwis okresowy', 80, 150, 30),
    ('Przegląd okresowy', 'Serwis okresowy', 150, 350, 60),
    ('Wymiana filtra powietrza', 'Serwis okresowy', 30, 60, 15),
    ('Wymiana filtra kabinowego', 'Serwis okresowy', 30, 60, 15),
    ('Wymiana płynu hamulcowego', 'Serwis okresowy', 80, 150, 30),
    ('Wymiana płynu chłodniczego', 'Serwis okresowy', 100, 200, 45),
    ('Wymiana świec zapłonowych', 'Serwis okresowy', 60, 150, 30),
    ('Wymiana świec żarowych', 'Serwis okresowy', 100, 300, 60),
    ('Wymiana klocków hamulcowych przód', 'Hamulce', 100, 200, 45),
    ('Wymiana klocków hamulcowych tył', 'Hamulce', 80, 180, 45),
    ('Wymiana tarcz i klocków przód', 'Hamulce', 200, 400, 60),
    ('Wymiana tarcz i klocków tył', 'Hamulce', 180, 350, 60),
    ('Wymiana szczęk hamulcowych', 'Hamulce', 100, 200, 60),
    ('Wymiana opon (4 szt.)', 'Opony', 80, 160, 45),
    ('Wyważanie kół (4 szt.)', 'Opony', 40, 80, 30),
    ('Przechowywanie opon (sezon)', 'Opony', 60, 120, 15),
    ('Naprawa opony', 'Opony', 20, 50, 20),
    ('Geometria kół', 'Opony', 100, 200, 45),
    ('Wymiana amortyzatorów przód', 'Zawieszenie', 200, 500, 120),
    ('Wymiana amortyzatorów tył', 'Zawieszenie', 150, 400, 90),
    ('Wymiana wahacza', 'Zawieszenie', 150, 350, 90),
    ('Wymiana łącznika stabilizatora', 'Zawieszenie', 50, 120, 30),
    ('Wymiana końcówki drążka', 'Zawieszenie', 80, 180, 45),
    ('Wymiana paska rozrządu', 'Rozrząd', 400, 1200, 240),
    ('Wymiana zestawu rozrządu z pompą', 'Rozrząd', 600, 1800, 300),
    ('Wymiana paska klinowego', 'Rozrząd', 80, 200, 45),
    ('Wymiana sprzęgła', 'Sprzęgło', 500, 1500, 360),
    ('Wymiana koła dwumasowego', 'Sprzęgło', 800, 2500, 420),
    ('Wymiana rozrusznika', 'Układ elektryczny', 200, 500, 90),
    ('Wymiana alternatora', 'Układ elektryczny', 250, 600, 90),
    ('Diagnostyka komputerowa', 'Diagnostyka', 50, 150, 30),
    ('Kasowanie błędów', 'Diagnostyka', 30, 80, 15),
    ('Kontrola klimatyzacji', 'Klimatyzacja', 50, 100, 30),
    ('Serwis klimatyzacji', 'Klimatyzacja', 150, 350, 60),
    ('Ozonowanie wnętrza', 'Klimatyzacja', 50, 100, 30),
    ('Wymiana tłumika', 'Układ wydechowy', 150, 400, 60),
    ('Wymiana katalizatora', 'Układ wydechowy', 500, 2000, 120),
    ('Spawanie wydechu', 'Układ wydechowy', 50, 150, 30),
    ('Wymiana akumulatora', 'Elektryka', 30, 60, 15),
    ('Wymiana żarówki', 'Elektryka', 20, 80, 15),
    ('Lakierowanie elementu', 'Blacharstwo', 300, 1500, 480),
    ('Naprawa blacharsko-lakiernicza', 'Blacharstwo', 500, 5000, 960),
    ('Polerowanie lakieru', 'Blacharstwo', 200, 600, 240),
    ('Usuwanie wgnieceń PDR', 'Blacharstwo', 100, 500, 120),
    ('Przegląd techniczny', 'Przegląd', 99, 99, 30),
    ('Badanie techniczne + spaliny', 'Przegląd', 162, 162, 45),
    ('Mycie silnika', 'Inne', 80, 200, 60),
    ('Zabezpieczenie antykorozyjne podwozia', 'Inne', 200, 600, 120),
]

METODY_PLATNOSCI = ['gotówka', 'karta', 'przelew', 'BLIK', 'leasing', 'raty']
PLATNOSC_WAGI = [0.15, 0.40, 0.20, 0.15, 0.05, 0.05]

STATUSY_ZLECEN = ['nowe', 'w_trakcie', 'oczekuje_na_czesci', 'zakonczone', 'anulowane']
STATUSY_WAGI = [0.02, 0.03, 0.01, 0.92, 0.02]

TYPY_LOKALIZACJI = ['warsztat', 'sklep', 'warsztat_i_sklep']
TYP_LOKALIZACJI_WAGI = [0.30, 0.20, 0.50]

STANOWISKA = {
    'warsztat': ['mechanik', 'mechanik_senior', 'elektryk_samochodowy', 'blacharz', 'lakiernik', 'diagnosta'],
    'sklep': ['sprzedawca', 'sprzedawca_senior', 'kasjer', 'magazynier'],
    'zarzadzanie': ['kierownik_oddzialu', 'zastepca_kierownika', 'ksiegowy'],
}
