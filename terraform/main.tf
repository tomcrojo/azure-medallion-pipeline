# -----------------------------------------------------
# Resource Group
# -----------------------------------------------------

resource "azurerm_resource_group" "main" {
  name     = "rg-${var.project_prefix}-${var.environment}"
  location = var.location
  tags     = var.tags
}
