output "resource_group_name" {
  description = "Name of the created resource group."
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "Name of the bronze layer storage account."
  value       = azurerm_storage_account.bronze.name
}

output "storage_container_name" {
  description = "Name of the bronze raw data container."
  value       = azurerm_storage_container.bronze_raw.name
}

output "storage_account_id" {
  description = "Full Azure resource ID of the storage account."
  value       = azurerm_storage_account.bronze.id
  sensitive   = true
}

output "pg_host" {
  description = "PostgreSQL Flexible Server FQDN."
  value       = azurerm_postgresql_flexible_server.main.fqdn
  sensitive   = true
}

output "pg_port" {
  description = "PostgreSQL port."
  value       = 5432
}

output "pg_database" {
  description = "Name of the market_signals database."
  value       = azurerm_postgresql_flexible_server_database.signals.name
}

output "pg_admin_login" {
  description = "PostgreSQL administrator login."
  value       = azurerm_postgresql_flexible_server.main.administrator_login
  sensitive   = true
}

output "pg_admin_password" {
  description = "PostgreSQL administrator password (randomly generated)."
  value       = random_password.pg_admin.result
  sensitive   = true
}
