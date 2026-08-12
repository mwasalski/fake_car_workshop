-- =============================================================
-- EXTERNAL STORAGE ON ADLS Gen2 - fake_car_workshop
-- Chain of objects (Unity Catalog):
--   Azure Access Connector (managed identity)
--     -> STORAGE CREDENTIAL
--       -> EXTERNAL LOCATION  (abfss:// path + credential)
--         -> EXTERNAL VOLUME  (file dump, e.g. landing zone)
--         -> EXTERNAL TABLE   (Delta table on your own path)
-- =============================================================

-- -------------------------------------------------------------
-- 0. AZURE PREREQUISITES (one-time, done OUTSIDE Databricks)
-- -------------------------------------------------------------
-- a) Storage account with hierarchical namespace enabled (ADLS Gen2):
--
--    az storage account create \
--      --name carworkshopadls \
--      --resource-group rg-car-workshop \
--      --location westeurope \
--      --sku Standard_LRS \
--      --kind StorageV2 \
--      --enable-hierarchical-namespace true
--
--    az storage container create \
--      --account-name carworkshopadls \
--      --name landing
--
-- b) Databricks Access Connector (a managed identity UC will use):
--
--    az databricks access-connector create \
--      --name ac-car-workshop \
--      --resource-group rg-car-workshop \
--      --location westeurope \
--      --identity-type SystemAssigned
--
-- c) Grant the connector's managed identity access to the storage account
--    (role must be "Storage Blob Data Contributor"; principal-id comes from step b):
--
--    az role assignment create \
--      --assignee <access-connector-principal-id> \
--      --role "Storage Blob Data Contributor" \
--      --scope /subscriptions/<sub-id>/resourceGroups/rg-car-workshop/providers/Microsoft.Storage/storageAccounts/carworkshopadls
--
-- Required UC privileges below: CREATE STORAGE CREDENTIAL and
-- CREATE EXTERNAL LOCATION on the metastore (metastore admin has both).

-- -------------------------------------------------------------
-- 1. STORAGE CREDENTIAL (wraps the access connector)
-- -------------------------------------------------------------
-- Resource ID of the ACCESS CONNECTOR (not the storage account).
-- NOTE: this cannot be created via plain SQL with the managed identity
-- clause on all DBR versions - if it fails, create it in
-- Catalog Explorer > External Data > Credentials, then continue below.
CREATE STORAGE CREDENTIAL IF NOT EXISTS car_workshop_adls_cred
  WITH AZURE_MANAGED_IDENTITY (
    ACCESS_CONNECTOR_ID = '/subscriptions/<sub-id>/resourceGroups/rg-car-workshop/providers/Microsoft.Databricks/accessConnectors/ac-car-workshop'
  )
  COMMENT 'Managed identity credential for carworkshopadls storage account';

-- -------------------------------------------------------------
-- 2. EXTERNAL LOCATION (binds abfss path to the credential)
-- -------------------------------------------------------------
-- URL pattern: abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
CREATE EXTERNAL LOCATION IF NOT EXISTS car_workshop_landing
  URL 'abfss://landing@carworkshopadls.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL car_workshop_adls_cred)
  COMMENT 'Landing container on ADLS for file dumps and external tables';

-- Optional: let other users create objects under this location
-- GRANT CREATE EXTERNAL VOLUME ON EXTERNAL LOCATION car_workshop_landing TO `account users`;
-- GRANT CREATE EXTERNAL TABLE  ON EXTERNAL LOCATION car_workshop_landing TO `account users`;
-- GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION car_workshop_landing TO `account users`;

-- Smoke test: should list the container without errors
-- LIST 'abfss://landing@carworkshopadls.dfs.core.windows.net/';

-- -------------------------------------------------------------
-- 3. EXTERNAL VOLUME (file dump - use like any UC volume)
-- -------------------------------------------------------------
-- Path must be inside the external location and NOT overlap with
-- any table location.
CREATE EXTERNAL VOLUME IF NOT EXISTS car_workshop.fact.adls_landing
  LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/volumes/fact_landing'
  COMMENT 'External volume on ADLS - raw file dump (parquet/csv/json)';

-- Access from notebooks exactly like a managed volume:
--   /Volumes/car_workshop/fact/adls_landing/...
--   dbutils.fs.ls('/Volumes/car_workshop/fact/adls_landing/')
--   df.write.parquet('/Volumes/car_workshop/fact/adls_landing/some_dump/')

-- -------------------------------------------------------------
-- 4. EXTERNAL TABLE (Delta table living on ADLS)
-- -------------------------------------------------------------
-- Example: external copy of a fact table. DROP TABLE will NOT delete
-- the underlying files - clean the path manually if you recreate it.
CREATE TABLE IF NOT EXISTS car_workshop.fact.fact_sales_external (
  sale_id                  BIGINT,
  location_id              BIGINT,
  customer_id              BIGINT,
  employee_id              BIGINT,
  sale_date                DATE,
  total_net                DOUBLE,
  total_vat                DOUBLE,
  total_gross              DOUBLE,
  payment_method           STRING,
  status                   STRING
)
USING DELTA
LOCATION 'abfss://landing@carworkshopadls.dfs.core.windows.net/tables/fact_sales_external';
