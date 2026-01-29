############################################################################
# User groups
############################################################################
data "azuread_group" "redaction_engineers" {
  display_name = "${local.org}-redaction-system-developers"
  #security_enabled = true
}
data "azuread_service_principal" "deployment" {
  display_name = "Azure DevOps Pipelines - Redaction System - Deployment ${upper(var.environment)}"
}

data "azuread_service_principal" "ci" {
  display_name = "Azure DevOps Pipelines - Redaction System CI/CD"
}


############################################################################
# Virtual networks
############################################################################
data "azurerm_virtual_network" "tooling" {
  name                = var.tooling_config.network_name
  resource_group_name = var.tooling_config.network_rg

  provider = azurerm.tooling
}

############################################################################
# DNS zone
############################################################################

data "azurerm_private_dns_zone" "storage" {
  for_each            = { for idx, val in local.storage_subresources : idx => val }
  name                = "privatelink.${each.value}.core.windows.net"
  resource_group_name = var.tooling_config.network_rg
  provider            = azurerm.tooling

  tags = local.tags
}

data "azurerm_private_dns_zone" "function" {
  name                = "privatelink.azurewebsites.net"
  resource_group_name = var.tooling_config.network_rg
  provider            = azurerm.tooling

  tags = local.tags
}


data "azurerm_private_dns_zone" "ai" {
  name                = "privatelink.cognitiveservices.azure.com"
  resource_group_name = var.tooling_config.network_rg
  provider            = azurerm.tooling

  tags = local.tags
}


data "azurerm_private_dns_zone" "openai" {
  name                = "privatelink.openai.azure.com"
  resource_group_name = var.tooling_config.network_rg
  provider            = azurerm.tooling

  tags = local.tags
}

data "azurerm_private_dns_zone" "servicebus" {
  name                = "privatelink.servicebus.windows.net"
  resource_group_name = var.tooling_config.network_rg
  provider            = azurerm.tooling

  tags = local.tags
}
