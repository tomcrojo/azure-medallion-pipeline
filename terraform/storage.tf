# -----------------------------------------------------
# ADLS Gen2 Storage Account + Medallion Containers
# -----------------------------------------------------

resource "azurerm_storage_account" "datalake" {
  name                     = "st${var.project_prefix}${var.environment}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  is_hns_enabled = true # Required for ADLS Gen2

  min_tls_version = "TLS1_2"

  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 7
    }
  }

  tags = var.tags
}

# Bronze container — raw ingested data
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.datalake.id
}

# Silver container — cleaned & validated data
resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.datalake.id
}

# Gold container — aggregated business-ready data
resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.datalake.id
}

# Container for ADF pipeline logs and metadata
resource "azurerm_storage_data_lake_gen2_filesystem" "logs" {
  name               = "logs"
  storage_account_id = azurerm_storage_account.datalake.id
}
