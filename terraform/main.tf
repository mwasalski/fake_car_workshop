# =============================================================
# ADLS Gen2 + Unity Catalog external storage - fake_car_workshop
# Scope: infra + governance objects only. Tables stay in SQL
# (create_tables.sql / create_external_adls.sql section 4).
# =============================================================

# ---- Azure side --------------------------------------------------

resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
}

# Storage account names are globally unique - random suffix avoids collisions.
resource "random_string" "sa_suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

resource "azurerm_storage_account" "adls" {
  name                     = "${var.storage_account_prefix}${random_string.sa_suffix.result}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  # This is what makes it ADLS Gen2. Immutable after creation.
  is_hns_enabled = true

  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
}

resource "azurerm_storage_container" "landing" {
  name                  = var.container_name
  storage_account_id    = azurerm_storage_account.adls.id
  container_access_type = "private"
}

resource "azurerm_databricks_access_connector" "uc" {
  name                = var.access_connector_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "connector_blob_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# Azure RBAC propagation is eventually consistent; creating the external
# location immediately after the role assignment often 403s. Buffer it.
resource "time_sleep" "rbac_propagation" {
  depends_on      = [azurerm_role_assignment.connector_blob_contributor]
  create_duration = "120s"
}

# ---- Unity Catalog side -------------------------------------------

resource "databricks_storage_credential" "adls" {
  name    = "car_workshop_adls_cred"
  comment = "Managed identity credential for ${azurerm_storage_account.adls.name} (Terraform-managed)"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.uc.id
  }
}

resource "databricks_external_location" "landing" {
  name            = "car_workshop_landing"
  url             = "abfss://${azurerm_storage_container.landing.name}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.adls.name
  comment         = "Landing container on ADLS for file dumps and external tables (Terraform-managed)"

  depends_on = [time_sleep.rbac_propagation]
}

resource "databricks_volume" "adls_landing" {
  name             = "adls_landing"
  catalog_name     = var.catalog_name
  schema_name      = var.schema_name
  volume_type      = "EXTERNAL"
  storage_location = "${databricks_external_location.landing.url}volumes/fact_landing"
  comment          = "External volume on ADLS - raw file dump (Terraform-managed)"
}

# Governance also belongs here - example (uncomment and adjust group name):
# resource "databricks_grants" "landing_location" {
#   external_location = databricks_external_location.landing.id
#   grant {
#     principal  = "data engineers"
#     privileges = ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE"]
#   }
# }
