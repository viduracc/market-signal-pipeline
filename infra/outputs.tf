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
