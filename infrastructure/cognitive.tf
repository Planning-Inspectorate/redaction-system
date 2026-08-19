resource "azurerm_cognitive_account" "open_ai" {
  #checkov:skip=CKV2_AZURE_22: Customer Managed Keys not implemented
  name                               = "${local.org}-openai-${local.ai_resource_suffix}"
  location                           = local.ai_location
  resource_group_name                = azurerm_resource_group.primary.name
  kind                               = "OpenAI"
  sku_name                           = "S0"
  custom_subdomain_name              = "${local.org}-openai-${local.ai_resource_suffix}"
  public_network_access_enabled      = false
  outbound_network_access_restricted = true
  fqdns                              = ["azureprivatedns.net"]
  local_auth_enabled                 = false
  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_cognitive_deployment" "open_ai" {
  name                 = "gpt-4.1"
  cognitive_account_id = azurerm_cognitive_account.open_ai.id

  model {
    format  = "OpenAI"
    name    = "gpt-4.1"
    version = "2025-04-14"
  }

  sku {
    name     = "DataZoneStandard"
    capacity = var.openai_quota
  }
}

resource "azurerm_cognitive_deployment" "open_ai_gpt_56_luna" {
  name                 = "gpt-5.6-luna"
  cognitive_account_id = azurerm_cognitive_account.open_ai.id

  model {
    format  = "OpenAI"
    name    = "gpt-5.6-luna"
    version = "2026-07-09"
  }

  sku {
    name     = "DataZoneStandard"
    capacity = var.openai_quotas.gpt_56_luna
  }
}

resource "azurerm_cognitive_account" "computer_vision" {
  #checkov:skip=CKV2_AZURE_22: Customer Managed Keys not implemented
  name                               = "${local.org}-cv-${local.resource_suffix}"
  location                           = local.location
  resource_group_name                = azurerm_resource_group.primary.name
  kind                               = "ComputerVision"
  sku_name                           = "F0"
  custom_subdomain_name              = "${local.org}-computervision-${local.resource_suffix}"
  public_network_access_enabled      = false
  outbound_network_access_restricted = true
  fqdns                              = ["azureprivatedns.net"]
  local_auth_enabled                 = false
  identity {
    type = "SystemAssigned"
  }
}