- Znalezisko przy okazji (ważniejsze niż porządki): autoloader.ipynb duplikuje cały słownik TABLE_SCHEMAS inline zamiast importować z table_schemas.py — to łamie zasadę single source of truth z CLAUDE.md. Każda zmiana schematu wymaga dziś edycji w dwóch miejscach (a właściwie trzech, licząc create_tables.sql). Warto to naprawić przed jakąkolwiek reorganizacją.

- osobne repo dla asset bundles

- 