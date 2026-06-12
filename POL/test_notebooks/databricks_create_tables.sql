-- =============================================================
-- TWORZENIE TABEL DELTA - fake_car_workshop
-- Katalog : fake_car_workshop_franchise_pl
-- Schematy : dim  (wymiarowe)
--            fact (faktowe)
-- =============================================================
-- Tworzenie catalogu
CREATE CATALOG IF NOT EXISTS fake_car_workshop_franchise_pl;
-- CREATE CATALOG IF NOT EXISTS fake_car_workshop_franchise_pl;


-- Tworzenie schematów
CREATE SCHEMA IF NOT EXISTS fake_car_workshop_franchise_pl.dim;
CREATE SCHEMA IF NOT EXISTS fake_car_workshop_franchise_pl.fact;


CREATE VOLUME IF NOT EXISTS fake_car_workshop_franchise_pl.dim.autoloader_checkpoints;
CREATE VOLUME IF NOT EXISTS fake_car_workshop_franchise_pl.fact.autoloader_checkpoints;

-- ============================================================
-- TWORZENIE WOLUMINÓW
-- ============================================================
CREATE VOLUME IF NOT EXISTS  fake_car_workshop_franchise_pl.dim.dim_daily_files;
CREATE VOLUME IF NOT EXISTS  fake_car_workshop_franchise_pl.fact.fact_daily_files;

-- =============================================================
-- TABELE WYMIAROWE
-- =============================================================

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.dim.dim_customers (
  customer_id                BIGINT,
  customer_code              STRING,
  typ_klienta                STRING,
  imie                       STRING,
  nazwisko                   STRING,
  nazwa_firmy                STRING,
  nip                        STRING,
  email                      STRING,
  telefon                    STRING,
  miasto                     STRING,
  kod_pocztowy               STRING,
  data_rejestracji           DATE,
  preferowana_lokalizacja_id BIGINT,
  zgoda_marketing            BOOLEAN
)
USING DELTA;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.dim.dim_employees (
  employee_id        BIGINT,
  employee_code      STRING,
  imie               STRING,
  nazwisko           STRING,
  pesel              STRING,
  stanowisko         STRING,
  location_id        BIGINT,
  data_zatrudnienia  DATE,
  data_zwolnienia    DATE,
  stawka_godzinowa   DOUBLE,
  czy_aktywny        BOOLEAN
)
USING DELTA;


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.dim.dim_locations (
  location_id        BIGINT,
  location_code      STRING,
  nazwa              STRING,
  typ                STRING,
  ulica              STRING,
  miasto             STRING,
  wojewodztwo        STRING,
  kod_pocztowy       STRING,
  latitude           DOUBLE,
  longitude          DOUBLE,
  telefon            STRING,
  email              STRING,
  kierownik_id       BIGINT,
  liczba_stanowisk   BIGINT,
  powierzchnia_m2    BIGINT,
  data_otwarcia      DATE,
  czy_aktywna        BOOLEAN
)
USING DELTA;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.dim.dim_products (
  product_id            BIGINT,
  product_code          STRING,
  nazwa                 STRING,
  kategoria             STRING,
  producent             STRING,
  cena_zakupu_netto     DOUBLE,
  cena_sprzedazy_netto  DOUBLE,
  vat_procent           BIGINT,
  jednostka             STRING,
  waga_kg               DOUBLE,
  min_stan_magazynowy   BIGINT,
  czy_aktywny           BOOLEAN
)
USING DELTA;


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.dim.dim_services (
  service_id          BIGINT,
  service_code        STRING,
  nazwa               STRING,
  kategoria           STRING,
  cena_min_netto      BIGINT,
  cena_max_netto      BIGINT,
  szacowany_czas_min  BIGINT,
  czy_aktywna         BOOLEAN
)
USING DELTA;


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.dim.dim_suppliers (
  supplier_id            BIGINT,
  supplier_code          STRING,
  nazwa                  STRING,
  nip                    STRING,
  miasto                 STRING,
  adres                  STRING,
  kod_pocztowy           STRING,
  telefon                STRING,
  email                  STRING,
  osoba_kontaktowa       STRING,
  warunki_platnosci_dni  BIGINT,
  min_wartosc_zamowienia DOUBLE,
  czy_aktywny            BOOLEAN
)
USING DELTA;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.dim.dim_vehicles (
  vehicle_id                  BIGINT,
  customer_id                 BIGINT,
  marka                       STRING,
  model                       STRING,
  rocznik                     BIGINT,
  vin                         STRING,
  nr_rejestracyjny            STRING,
  typ_paliwa                  STRING,
  pojemnosc_silnika           DOUBLE,
  moc_km                      BIGINT,
  kolor                       STRING,
  przebieg_km                 BIGINT,
  data_pierwszej_rejestracji  DATE
)
USING DELTA;


-- =============================================================
-- TABELE FAKTOWE
-- Partycjonowane (rok/miesiac): appointments, inventory_movements,
--   invoices, payments, sales_transactions, work_orders
-- Niepartycjonowane: pozostałe
-- =============================================================

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_appointments (
  appointment_id   BIGINT,
  customer_id      BIGINT,
  vehicle_id       BIGINT,
  location_id      BIGINT,
  service_id       BIGINT,
  data_rezerwacji  DATE,
  data_wizyty      DATE,
  status           STRING,
  kanal_rezerwacji STRING,
  uwagi            STRING,
  rok              INT,
  miesiac          INT
)
USING DELTA
PARTITIONED BY (rok, miesiac);


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_customer_feedback (
  feedback_id   BIGINT,
  customer_id   BIGINT,
  location_id   BIGINT,
  work_order_id BIGINT,
  data_opinii   DATE,
  ocena         BIGINT,
  komentarz     STRING,
  kategoria     STRING,
  kanal         STRING
)
USING DELTA;


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_employee_schedules (
  schedule_id   BIGINT,
  employee_id   BIGINT,
  data          DATE,
  godzina_start BIGINT,
  godzina_koniec BIGINT,
  typ_zmiany    STRING,
  nadgodziny_h  BIGINT,
  obecnosc      STRING
)
USING DELTA;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_inventory_movements (
  movement_id        BIGINT,
  product_id         BIGINT,
  location_id        BIGINT,
  typ_ruchu          STRING,
  ilosc              BIGINT,
  data_ruchu         DATE,
  dokument_zrodlowy  STRING,
  nr_dokumentu       STRING,
  wartosc_netto      DOUBLE,
  uwagi              STRING,
  rok                INT,
  miesiac            INT
)
USING DELTA
PARTITIONED BY (rok, miesiac);

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_invoices (
  invoice_id      BIGINT,
  invoice_code    STRING,
  typ_dokumentu   STRING,
  source_type     STRING,
  source_id       BIGINT,
  customer_id     BIGINT,
  location_id     BIGINT,
  data_wystawienia DATE,
  data_sprzedazy  DATE,
  termin_platnosci DATE,
  wartosc_netto   DOUBLE,
  wartosc_vat     DOUBLE,
  wartosc_brutto  DOUBLE,
  status          STRING,
  rok             INT,
  miesiac         INT
)
USING DELTA
PARTITIONED BY (rok, miesiac);

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_loyalty_program (
  loyalty_id     BIGINT,
  customer_id    BIGINT,
  data_zdarzenia DATE,
  typ_zdarzenia  STRING,
  punkty         BIGINT,
  opis           STRING,
  saldo_po       BIGINT,
  poziom         STRING
)
USING DELTA;


-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_payments (
  payment_id        BIGINT,
  invoice_id        BIGINT,
  data_platnosci    DATE,
  kwota             DOUBLE,
  metoda_platnosci  STRING,
  status            STRING,
  numer_transakcji  STRING,
  rok               INT,
  miesiac           INT
)
USING DELTA
PARTITIONED BY (rok, miesiac);

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_purchase_order_items (
  po_item_id            BIGINT,
  po_id                 BIGINT,
  product_id            BIGINT,
  ilosc_zamowiona       BIGINT,
  ilosc_dostarczona     BIGINT,
  cena_jednostkowa_netto DOUBLE,
  wartosc_netto         DOUBLE
)
USING DELTA;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_purchase_orders (
  po_id                    BIGINT,
  po_code                  STRING,
  supplier_id              BIGINT,
  location_id              BIGINT,
  data_zamowienia          DATE,
  data_dostawy_planowana   DATE,
  data_dostawy_rzeczywista DATE,
  wartosc_netto            DOUBLE,
  wartosc_brutto           DOUBLE,
  status                   STRING,
  rok                      INT
)
USING DELTA
PARTITIONED BY (rok);

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_sales_items (
  sales_item_id          BIGINT,
  transaction_id         BIGINT,
  product_id             BIGINT,
  ilosc                  BIGINT,
  cena_jednostkowa_netto DOUBLE,
  rabat_procent          BIGINT,
  wartosc_netto          DOUBLE,
  vat_procent            BIGINT,
  wartosc_brutto         DOUBLE
)
USING DELTA;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_sales_transactions (
  transaction_id    BIGINT,
  transaction_code  STRING,
  location_id       BIGINT,
  customer_id       BIGINT,
  employee_id       BIGINT,
  data_transakcji   DATE,
  metoda_platnosci  STRING,
  nr_paragonu       STRING,
  rok               BIGINT,
  miesiac           BIGINT
)
USING DELTA
PARTITIONED BY (rok, miesiac);

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_work_order_items (
  wo_item_id             BIGINT,
  work_order_id          BIGINT,
  typ_pozycji            STRING,
  service_id             BIGINT,
  product_id             BIGINT,
  ilosc                  BIGINT,
  cena_jednostkowa_netto DOUBLE,
  wartosc_netto          DOUBLE,
  vat_procent            BIGINT,
  wartosc_brutto         DOUBLE,
  rabat_procent          BIGINT
)
USING DELTA;

-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fake_car_workshop_franchise_pl.fact.fact_work_orders (
  work_order_id          BIGINT,
  work_order_code        STRING,
  location_id            BIGINT,
  customer_id            BIGINT,
  vehicle_id             BIGINT,
  mechanic_id            BIGINT,
  data_przyjecia         DATE,
  data_zakonczenia       DATE,
  status                 STRING,
  przebieg_przy_przyjecia BIGINT,
  uwagi_klienta          STRING,
  rok                    INT,
  miesiac                INT
)
USING DELTA
PARTITIONED BY (rok, miesiac);
