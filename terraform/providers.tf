provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  subscription_id = var.subscription_id
}

provider "databricks" {
  host = azurerm_databricks_workspace.main.workspace_url
}
