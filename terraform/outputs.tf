output "storage_account_name" {
  description = "Generated storage account name (random suffix included)."
  value       = azurerm_storage_account.adls.name
}

output "access_connector_id" {
  description = "Resource ID of the access connector (what CREATE STORAGE CREDENTIAL would need)."
  value       = azurerm_databricks_access_connector.uc.id
}

output "external_location_url" {
  description = "abfss:// root governed by Unity Catalog."
  value       = databricks_external_location.landing.url
}

output "volume_path" {
  description = "Path usable from notebooks like any UC volume."
  value       = "/Volumes/${var.catalog_name}/${var.schema_name}/${databricks_volume.adls_landing.name}"
}
