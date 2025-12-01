# -----------------------------------------------------
# Global Variables
# -----------------------------------------------------

variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
  sensitive   = true
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "westeurope"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_prefix" {
  description = "Prefix for all resource names (lowercase, no spaces)"
  type        = string
  default     = "medallion"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{2,12}$", var.project_prefix))
    error_message = "Project prefix must be 3-13 lowercase alphanumeric characters."
  }
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    Project     = "azure-medallion-pipeline"
    ManagedBy   = "terraform"
    Environment = "dev"
  }
}

# Databricks
variable "databricks_sku" {
  description = "Databricks workspace SKU (standard, premium)"
  type        = string
  default     = "standard"
}

# Networking
variable "vnet_address_space" {
  description = "Address space for the virtual network"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

variable "private_subnet_prefix" {
  description = "Address prefix for Databricks private subnet"
  type        = string
  default     = "10.0.1.0/24"
}

variable "public_subnet_prefix" {
  description = "Address prefix for Databricks public subnet"
  type        = string
  default     = "10.0.2.0/24"
}
