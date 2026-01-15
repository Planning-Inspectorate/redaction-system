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

  delegation {
    name = "RedactionSubnetDelegation"
    service_delegation {
      name    = "Microsoft.Web/serverFarms"
      actions = "Microsoft.Network/virtualNetworks/subnets/action"
    }
  }
}
