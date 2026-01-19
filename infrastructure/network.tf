/*
############################################################################
# Virtual networks
############################################################################
resource "azurerm_virtual_network" "redaction_system" {
  name                = "vnet-redaction-system-${var.environment}-${local.location_short}"
  location            = local.location
  resource_group_name = azurerm_resource_group.redaction_rg.name
  address_space       = [var.vnet_cidr_block]

  tags = local.tags
}

resource "azurerm_subnet" "redaction_system" {
  name                 = "RedactionSubnet"
  resource_group_name  = azurerm_resource_group.redaction_rg.name
  address_prefixes     = [var.subnet_cidr_block]
  virtual_network_name = azurerm_virtual_network.redaction_system.name
  service_endpoints    = ["Microsoft.Storage"]
  private_endpoint_network_policies = "Enabled"
}

resource "azurerm_subnet" "function_app" {
  name                 = "FunctionAppSubnet"
  resource_group_name  = azurerm_resource_group.redaction_rg.name
  address_prefixes     = [var.functionapp_cidr_block]
  virtual_network_name = azurerm_virtual_network.redaction_system.name
  service_endpoints    = ["Microsoft.Storage"]
  private_endpoint_network_policies = "Enabled"

  delegation {
    name = "functionAppDelegation"
    service_delegation {
      name    = "Microsoft.Web/serverFarms"
      actions = ["Microsoft.Network/virtualNetworks/subnets/action"]
    }
  }
}

data "azurerm_virtual_network" "tooling" {
  name                = local.tooling_config.network_name
  resource_group_name = local.tooling_config.network_rg

  provider = azurerm.tooling
}

############################################################################
# DNS zone
############################################################################

data "azurerm_private_dns_zone" "blob" {
  name                = "privatelink.blob.core.windows.net"
  resource_group_name = local.tooling_config.network_rg
  provider            = azurerm.tooling

  tags = local.tags
}

data "azurerm_private_dns_zone" "function" {
  name                = "privatelink.azurewebsites.net"
  resource_group_name = local.tooling_config.network_rg
  provider            = azurerm.tooling

  tags = local.tags
}


data "azurerm_private_dns_zone" "ai" {
  name                = "privatelink.cognitiveservices.azure.com"
  resource_group_name = local.tooling_config.network_rg
  provider            = azurerm.tooling

  tags = local.tags
}

############################################################################
# Private endpoints
############################################################################
resource "azurerm_private_endpoint" "redaction_storage" {
  name                = "pins-pe-${azurerm_storage_account.redaction_storage.name}"
  resource_group_name = azurerm_resource_group.redaction_rg.name
  location            = local.location
  subnet_id           = azurerm_subnet.redaction_system.id

  private_dns_zone_group {
    name                 = "pins-pdns-${local.service_name}-storage-${var.environment}"
    private_dns_zone_ids = [data.azurerm_private_dns_zone.blob.id]
  }

  private_service_connection {
    name                           = "pins-psc-${local.service_name}-storage-${var.environment}"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_storage_account.redaction_storage.id
    subresource_names              = ["blob"]
  }

  tags = local.tags
}

resource "azurerm_private_endpoint" "function_app" {
  name                = "pins-pe-${azurerm_linux_function_app.redaction_system.name}"
  resource_group_name = azurerm_resource_group.redaction_rg.name
  location            = local.location
  subnet_id           = azurerm_subnet.redaction_system.id

  private_dns_zone_group {
    name                 = "pins-pdns-${local.service_name}-functionapp-${var.environment}"
    private_dns_zone_ids = [data.azurerm_private_dns_zone.function.id]
  }

  private_service_connection {
    name                           = "pins-psc-${local.service_name}-functionapp-${var.environment}"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_linux_function_app.redaction_system.id
    subresource_names              = ["sites"]
  }

  tags = local.tags
}

resource "azurerm_private_endpoint" "open_ai" {
  name                = "pins-pe-${azurerm_cognitive_account.open_ai.name}"
  resource_group_name = azurerm_resource_group.redaction_rg.name
  location            = local.location
  subnet_id           = azurerm_subnet.redaction_system.id

  private_dns_zone_group {
    name                 = "pins-pdns-${local.service_name}-openai-cognitive-${var.environment}"
    private_dns_zone_ids = [data.azurerm_private_dns_zone.ai.id]
  }

  private_service_connection {
    name                           = "pins-psc-${local.service_name}-openai-cognitive-${var.environment}"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_cognitive_account.open_ai.id
    subresource_names              = ["account"]
  }

  tags = local.tags
}

resource "azurerm_private_endpoint" "computer_vision" {
  name                = "pins-pe-${azurerm_cognitive_account.computer_vision.name}"
  resource_group_name = azurerm_resource_group.redaction_rg.name
  location            = local.location
  subnet_id           = azurerm_subnet.redaction_system.id

  private_dns_zone_group {
    name                 = "pins-pdns-${local.service_name}-computervision-${var.environment}"
    private_dns_zone_ids = [data.azurerm_private_dns_zone.ai.id]
  }

  private_service_connection {
    name                           = "pins-psc-${local.service_name}-computervision-${var.environment}"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_cognitive_account.computer_vision.id
    subresource_names              = ["account"]
  }

  tags = local.tags
}


############################################################################
# Network peering
############################################################################
resource "azurerm_virtual_network_peering" "redaction_to_tooling" {
  name                      = "pins-peer-redaction-system-to-tooling-${var.environment}"
  resource_group_name       = azurerm_resource_group.redaction_rg.name
  virtual_network_name      = azurerm_virtual_network.redaction_system.name
  remote_virtual_network_id = data.azurerm_virtual_network.tooling.id
}

resource "azurerm_virtual_network_peering" "tooling_to_redaction" {
  name                      = "pins-peer-tooling-to-redaction-system-${var.environment}"
  resource_group_name       = var.tooling_config.network_rg
  virtual_network_name      = var.tooling_config.network_name
  remote_virtual_network_id = azurerm_virtual_network.redaction_system.id

  provider = azurerm.tooling
}

*/