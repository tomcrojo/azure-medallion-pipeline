# -----------------------------------------------------
# Outputs
# -----------------------------------------------------

output "resource_group_name" {
  description = "Name of the resource group"
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = azurerm_storage_account.datalake.name
}

output "storage_account_id" {
  description = "ADLS Gen2 storage account ID"
  value       = azurerm_storage_account.datalake.id
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "databricks_workspace_id" {
  description = "Databricks workspace Azure resource ID"
  value       = azurerm_databricks_workspace.main.workspace_id
}

output "data_factory_name" {
  description = "Azure Data Factory name"
  value       = azurerm_data_factory.main.name
}

output "data_factory_id" {
  description = "Azure Data Factory ID"
  value       = azurerm_data_factory.main.id
}

output "vnet_id" {
  description = "Virtual network ID"
  value       = azurerm_virtual_network.main.id
}
