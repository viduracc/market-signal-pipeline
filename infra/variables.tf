variable "project_name" {
  description = "Short identifier used in resource names. Lowercase, no spaces."
  type        = string
  default     = "msp"

  validation {
    condition     = can(regex("^[a-z0-9]{2,10}$", var.project_name))
    error_message = "project_name must be 2-10 lowercase alphanumeric characters."
  }
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "eastus"
}

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default = {
    project    = "market-signal-pipeline"
    managed_by = "terraform"
  }
}

variable "pg_admin_login" {
  description = "Administrator login for the PostgreSQL Flexible Server."
  type        = string
  default     = "pgadmin"

  validation {
    condition     = !contains(["admin", "administrator", "azure_superuser", "azure_pg_admin", "root", "guest", "public", "postgres"], var.pg_admin_login)
    error_message = "pg_admin_login must not be a reserved PostgreSQL or Azure name."
  }
}

variable "pg_allowed_ip" {
  description = "Developer machine WAN IP allowed through the Postgres firewall. Set in terraform.tfvars."
  type        = string

  validation {
    condition     = can(regex("^(\\d{1,3}\\.){3}\\d{1,3}$", var.pg_allowed_ip))
    error_message = "pg_allowed_ip must be a valid IPv4 address."
  }
}

variable "pg_location" {
  description = "Azure region for the PostgreSQL Flexible Server."
  type        = string
  default     = "southeastasia"
}
