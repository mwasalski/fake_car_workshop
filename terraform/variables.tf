variable "subscription_id" {
  description = "Azure subscription ID (az account show --query id -o tsv)."
  type        = string
}

variable "databricks_workspace_url" {
  description = "Workspace URL, e.g. https://adb-1234567890123456.7.azuredatabricks.net"
  type        = string
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "westeurope"
}

variable "resource_group_name" {
  description = "Resource group holding the storage account and access connector."
  type        = string
  default     = "rg-car-workshop"
}

variable "storage_account_prefix" {
  description = "Storage account name prefix; a random suffix is appended (global uniqueness)."
  type        = string
  default     = "carworkshopadls"
}

variable "container_name" {
  description = "ADLS container used as the external location root."
  type        = string
  default     = "landing"
}

variable "access_connector_name" {
  description = "Databricks access connector (managed identity) name."
  type        = string
  default     = "ac-car-workshop"
}

variable "catalog_name" {
  description = "Existing UC catalog (created by create_tables.sql, not by Terraform)."
  type        = string
  default     = "car_workshop"
}

variable "schema_name" {
  description = "Existing UC schema for the external volume."
  type        = string
  default     = "fact"
}
