# -----------------------------------------------------
# Azure Data Factory + Linked Service to ADLS Gen2
# -----------------------------------------------------

resource "azurerm_data_factory" "main" {
  name                = "adf-${var.project_prefix}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Managed Identity: grant ADF access to the storage account
resource "azurerm_role_assignment" "adf_storage_blob" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# Linked service: ADF → ADLS Gen2 (using managed identity)
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "datalake" {
  name                = "ls_adls_gen2"
  data_factory_id     = azurerm_data_factory.main.id
  url                 = "https://${azurerm_storage_account.datalake.name}.dfs.core.windows.net"
  use_managed_identity = true
}

# Linked service: ADF → Databricks
resource "azurerm_data_factory_linked_service_azure_databricks" "databricks" {
  name            = "ls_databricks"
  data_factory_id = azurerm_data_factory.main.id
  adb_domain      = "https://${azurerm_databricks_workspace.main.workspace_url}"

  # Use existing cluster (set after first notebook run creates a cluster)
  # For production, configure a job cluster here instead
  existing_cluster_id = "" # Populate after Databricks cluster is created

  lifecycle {
    ignore_changes = [existing_cluster_id]
  }
}

# Pipeline: Databricks notebook execution (reference to ADF pipeline JSON)
# In production, you would deploy this via ARM/azapi or the ADF CI/CD process.
# The JSON definition is in adf/pipeline_ingestion.json.
