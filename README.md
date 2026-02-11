# Generator danych - Sieć Warsztatów Samochodowych i Sklepów z Akcesoriami

## Opis projektu

Generator realistycznych danych biznesowych symulujących działalność sieci **100 warsztatów samochodowych i sklepów z akcesoriami** rozlokowanych w miastach w całej Polsce. Dane obejmują okres **5 lat (2020-2024)** i mogą osiągnąć rozmiar **~30 GB** przy pełnej skali.

Projekt został stworzony do nauki pracy z dużymi zbiorami danych w **Databricks** (partycjonowanie, Delta Lake, Z-ordering, optymalizacja zapytań).

---

## Wymagania

```bash
pip install pandas pyarrow faker tqdm numpy
```

Python 3.9+

---

## Szybki start

1. Otwórz notebook `warsztat_generator.ipynb`
2. W komórce **KONFIGURACJA** ustaw parametry:

```python
SCALE_FACTOR = 0.01    # 0.01 = ~300MB, 0.1 = ~3GB, 1.0 = ~30GB
OUTPUT_DIR = './output_data'
OUTPUT_FORMAT = 'parquet'  # lub 'csv'
```

3. Uruchom wszystkie komórki (Run All)
4. Dane pojawią się w katalogu `./output_data/`

---

## Konfiguracja skali

| SCALE_FACTOR | Rozmiar danych | Czas generowania* | Zastosowanie |
|:---:|:---:|:---:|---|
| `0.01` | ~300 MB | ~2-5 min | Szybkie testy, prototypowanie |
| `0.1` | ~3 GB | ~20-40 min | Nauka partycjonowania |
| `0.5` | ~15 GB | ~2-3 h | Testy wydajności |
| `1.0` | ~30 GB | ~4-6 h | Pełne dane produkcyjne |

*\*Orientacyjny czas na maszynie z 16 GB RAM*

---

## Model danych

### Schemat relacji

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

### Tabele wymiarowe (dimension tables)

| Tabela | Wiersze | Opis |
|---|:---:|---|
| `dim_locations` | 100 | Lokalizacje warsztatów/sklepów - miasto, typ (warsztat/sklep/oba), adres, współrzędne GPS |
| `dim_employees` | ~2,000 | Pracownicy - mechanicy, sprzedawcy, kierownicy, diagności; przypisani do lokalizacji |
| `dim_customers` | 500K | Klienci indywidualni (70%) i firmowi (30%) z NIP |
| `dim_vehicles` | 600K | Pojazdy klientów - 20 marek, modele, VIN, paliwo, przebieg |
| `dim_products` | ~15,000 | Produkty w 15 kategoriach (oleje, filtry, opony, akumulatory, chemia, akcesoria...) z wariantami producentów |
| `dim_services` | 48 | Katalog usług warsztatowych z cenami min/max i czasem realizacji |
| `dim_suppliers` | 300 | Dostawcy części z warunkami płatności |

### Tabele faktowe (fact tables)

| Tabela | Wiersze (SCALE=1.0) | Opis |
|---|:---:|---|
| `fact_work_orders` | 5M | Zlecenia warsztatowe - klient, pojazd, mechanik, status, uwagi |
| `fact_work_order_items` | 15M | Pozycje zleceń - usługi (40%) i części (60%) z cenami i VAT |
| `fact_sales_transactions` | 30M | Transakcje sprzedaży sklepowej - paragon, metoda płatności |
| `fact_sales_items` | 90M | Pozycje sprzedaży - produkt, ilość, cena, rabat |
| `fact_invoices` | 35M | Faktury VAT, paragony, korekty - powiązane z zleceniami i sprzedażą |
| `fact_payments` | 35M | Płatności - gotówka, karta, przelew, BLIK, leasing |
| `fact_inventory_movements` | 50M | Ruchy magazynowe - przyjęcia (PZ), wydania (WZ), zwroty, inwentaryzacja |

### Tabele wspierające

| Tabela | Wiersze (SCALE=1.0) | Opis |
|---|:---:|---|
| `fact_appointments` | 5M | Rezerwacje wizyt - kanał (telefon/online/osobiscie), status |
| `fact_purchase_orders` | 500K | Zamówienia do dostawców z planowaną i rzeczywistą datą dostawy |
| `fact_purchase_order_items` | 2M | Pozycje zamówień - ilość zamówiona vs dostarczona |
| `fact_customer_feedback` | 2M | Opinie klientów - ocena 1-5, komentarze, kategoria |
| `fact_loyalty_program` | 500K | Program lojalnościowy - punkty, poziomy (standard/silver/gold/platinum) |
| `fact_employee_schedules` | 3M | Grafiki pracy - zmiany, nadgodziny, obecność |

---

## Klucze i relacje (FK)

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

## Wbudowany realizm danych

- **Sezonowość** - więcej zleceń w marcu/kwietniu (wymiana opon na letnie) i październiku/listopadzie (na zimowe)
- **Rozkład cen** - lognormalny (większość tanich transakcji, mniej drogich)
- **Popularność marek** - Toyota 12%, VW 11%, Skoda 10%... zgodne z polskim rynkiem
- **Typy paliwa** - benzyna 35%, diesel 30%, LPG 15%, hybryda 15%, elektryczny 5%
- **Metody płatności** - karta 40%, przelew 20%, gotówka 15%, BLIK 15%
- **Godziny pracy** - rozkład transakcji odpowiada godzinom otwarcia (7-19)
- **Polskie dane** - imiona, nazwiska, PESEL, NIP, adresy, miasta (Faker pl_PL)

---

## Struktura plików wyjściowych

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
├── fact_work_orders/              ← partycjonowane
│   ├── rok=2020/miesiac=1/
│   ├── rok=2020/miesiac=2/
│   └── ...
├── fact_work_order_items/
├── fact_sales_transactions/       ← partycjonowane
│   ├── rok=2020/miesiac=1/
│   └── ...
├── fact_sales_items/
├── fact_invoices/                 ← partycjonowane
├── fact_payments/                 ← partycjonowane
├── fact_inventory_movements/      ← partycjonowane
├── fact_appointments/             ← partycjonowane
├── fact_purchase_orders/
├── fact_purchase_order_items/
├── fact_customer_feedback/
├── fact_loyalty_program/
└── fact_employee_schedules/
```

Tabele faktowe z datami są partycjonowane po `rok/miesiac` - idealne do nauki partition pruning w Databricks.

---

## Ładowanie do Databricks

### Opcja 1: DBFS upload + Spark

```python
# Po wgraniu do DBFS
df_locations = spark.read.parquet("dbfs:/FileStore/output_data/dim_locations/")
df_work_orders = spark.read.parquet("dbfs:/FileStore/output_data/fact_work_orders/")

# Tabele partycjonowane automatycznie rozpoznają kolumny rok/miesiac
df_work_orders.printSchema()
```

### Opcja 2: Unity Catalog Volume

```python
df = spark.read.parquet("/Volumes/catalog/schema/volume/output_data/dim_customers/")
```

### Opcja 3: Managed table z Delta

```sql
CREATE TABLE warsztat.dim_locations
USING DELTA
AS SELECT * FROM parquet.`dbfs:/FileStore/output_data/dim_locations/`;

CREATE TABLE warsztat.fact_work_orders
USING DELTA
PARTITIONED BY (rok, miesiac)
AS SELECT * FROM parquet.`dbfs:/FileStore/output_data/fact_work_orders/`;

-- Optymalizacja
OPTIMIZE warsztat.fact_work_orders ZORDER BY (location_id, customer_id);
```

---

## Przykładowe zapytania analityczne

```sql
-- Top 10 lokalizacji po przychodach
SELECT l.miasto, COUNT(*) AS zlecenia, SUM(i.wartosc_brutto) AS przychod
FROM fact_work_orders w
JOIN fact_work_order_items i ON w.work_order_id = i.work_order_id
JOIN dim_locations l ON w.location_id = l.location_id
GROUP BY l.miasto
ORDER BY przychod DESC
LIMIT 10;

-- Sezonowość zleceń
SELECT rok, miesiac, COUNT(*) AS liczba_zlecen
FROM fact_work_orders
GROUP BY rok, miesiac
ORDER BY rok, miesiac;

-- Najpopularniejsze usługi
SELECT s.nazwa, COUNT(*) AS ilosc
FROM fact_work_order_items wi
JOIN dim_services s ON wi.service_id = s.service_id
WHERE wi.typ_pozycji = 'usluga'
GROUP BY s.nazwa
ORDER BY ilosc DESC;

-- Średnia ocena per lokalizacja
SELECT l.miasto, ROUND(AVG(f.ocena), 2) AS avg_ocena, COUNT(*) AS n
FROM fact_customer_feedback f
JOIN dim_locations l ON f.location_id = l.location_id
GROUP BY l.miasto
ORDER BY avg_ocena DESC;
```
