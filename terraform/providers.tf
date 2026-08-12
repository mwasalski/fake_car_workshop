# Both providers authenticate through your local `az login` session -
# no secrets in code, no service principals needed for a sandbox.

provider "azurerm" {
  features {}
  # Required since azurerm 4.x (no longer inferred from az cli context).
  subscription_id = var.subscription_id
}

# Workspace-level provider: UC storage credentials / external locations are
# managed through a UC-enabled workspace. Your az-logged-in user must have
# the CREATE STORAGE CREDENTIAL / CREATE EXTERNAL LOCATION privileges on the
# metastore (metastore admin has both).
provider "databricks" {
  host      = var.databricks_workspace_url
  auth_type = "azure-cli"
}
