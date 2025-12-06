# -----------------------------------------------------
# Databricks Workspace
# -----------------------------------------------------

resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${var.project_prefix}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.databricks_sku

  # Standard tier: no VNet injection required.
  # For Premium, uncomment the managed_resource_group and VNet integration below.
  # managed_resource_group_name = "rg-${var.project_prefix}-dbw-managed-${var.environment}"

  public_network_access_enabled = true
  customer_managed_key_enabled  = false

  tags = var.tags
}
