resource "random_password" "pg_admin" {
  length           = 24
  special          = true
  override_special = "!#%&*-_=+[]{}<>:?"
  min_lower        = 2
  min_upper        = 2
  min_numeric      = 2
  min_special      = 2
}

resource "azurerm_postgresql_flexible_server" "main" {
  name                = "pg-${var.project_name}-${var.environment}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  version    = "16"
  sku_name   = "B_Standard_B1ms"
  storage_mb = 32768

  administrator_login    = var.pg_admin_login
  administrator_password = random_password.pg_admin.result

  backup_retention_days        = 7
  geo_redundant_backup_enabled = false

  authentication {
    password_auth_enabled         = true
    active_directory_auth_enabled = false
  }

  tags = var.tags
}

resource "azurerm_postgresql_flexible_server_database" "signals" {
  name      = "market_signals"
  server_id = azurerm_postgresql_flexible_server.main.id
  collation = "en_US.utf8"
  charset   = "UTF8"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "dev_machine" {
  name             = "allow-dev-machine"
  server_id        = azurerm_postgresql_flexible_server.main.id
  start_ip_address = var.pg_allowed_ip
  end_ip_address   = var.pg_allowed_ip
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "azure_services" {
  name             = "allow-azure-services"
  server_id        = azurerm_postgresql_flexible_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}
