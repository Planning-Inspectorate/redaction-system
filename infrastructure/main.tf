############################################################################
# Create resource groups
############################################################################
resource "azurerm_resource_group" "primary" {
  name     = "${local.org}-rg-${local.resource_suffix}"
  location = local.location
  tags     = local.tags
}

############################################################################
# Create storage account
############################################################################


#import {
#  to = azurerm_storage_share.function_app
#  id = "/subscriptions/962e477c-0f3b-4372-97fc-a198a58e259e/resourceGroups/pins-rg-redaction-system-dev-uks/providers/Microsoft.Storage/storageAccounts/pinsstredactiondevuks/fileServices/default/shares/pins-func-redaction-system-dev-uks"
#}

############################################################################
# Create Azure Function App
############################################################################

resource "azurerm_service_plan" "redaction_system" {
  #checkov:skip=CKV_AZURE_212: TODO: Limit reached in subscription
  #checkov:skip=CKV_AZURE_225: TODO: Limit reached in subscription
  name                = "${local.org}-asp-${local.resource_suffix}"
  resource_group_name = azurerm_resource_group.primary.name
  location            = local.location
  os_type             = "Linux"
  sku_name            = "EP1"
  #worker_count           = 2
  #zone_balancing_enabled = true
}

resource "azurerm_linux_function_app" "redaction_system" {
  name                = "${local.org}-func-${local.resource_suffix}"
  resource_group_name = azurerm_resource_group.primary.name
  location            = local.location

  storage_account_name          = null
  storage_account_access_key    = null
  service_plan_id               = azurerm_service_plan.redaction_system.id
  public_network_access_enabled = false
  virtual_network_subnet_id     = azurerm_subnet.function_app.id
  https_only                    = true

  site_config {
    application_stack {
      python_version = var.function_app_python_version
    }
    application_insights_key = azurerm_application_insights.redaction_system.instrumentation_key
    cors {
      allowed_origins = ["https://portal.azure.com"]
    }
  }
  identity {
    type = "SystemAssigned"
  }
  app_settings = {
    #"WEBSITE_CONTENTAZUREFILECONNECTIONSTRING" = "DefaultEndpointsProtocol=https;AccountName=${azurerm_storage_account.redaction_storage.name};AccountKey=${azurerm_storage_account.redaction_storage.primary_access_key};EndpointSuffix=core.windows.net"
    "WEBSITE_CONTENTSHARE" : "${local.org}-func-${local.resource_suffix}"
    "SCM_DO_BUILD_DURING_DEPLOYMENT" = "true"
    "OPENAI_ENDPOINT"                = azurerm_cognitive_account.open_ai.endpoint
    "AZURE_VISION_ENDPOINT"          = azurerm_cognitive_account.computer_vision.endpoint
    "ENV"                            = var.environment
    "APP_INSIGHTS_CONNECTION_STRING" = azurerm_application_insights.redaction_system.connection_string
    "WEBSITE_CONTENTOVERVNET"        = 1
  }
}

############################################################################
# Create App Insights
############################################################################
resource "azurerm_log_analytics_workspace" "redaction_system" {
  name                = "${local.org}-log-${local.resource_suffix}"
  location            = local.location
  resource_group_name = azurerm_resource_group.primary.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days
  daily_quota_gb      = var.daily_log_cap

  tags = local.tags
}

resource "azurerm_application_insights" "redaction_system" {
  name                = "${local.org}-ai-${local.resource_suffix}"
  location            = local.location
  resource_group_name = azurerm_resource_group.primary.name
  application_type    = "other"
  retention_in_days   = var.log_retention_days
  workspace_id        = azurerm_log_analytics_workspace.redaction_system.id

  tags = local.tags
}

############################################################################
# Create Azure Open AI
############################################################################
resource "azurerm_cognitive_account" "open_ai" {
  #checkov:skip=CKV2_AZURE_22: Customer Managed Keys not implemented
  name                               = "${local.org}-openai-${local.resource_suffix}"
  location                           = local.location
  resource_group_name                = azurerm_resource_group.primary.name
  kind                               = "OpenAI"
  sku_name                           = "S0"
  custom_subdomain_name              = "${local.org}-openai-${local.resource_suffix}"
  public_network_access_enabled      = false
  outbound_network_access_restricted = true
  fqdns                              = ["azureprivatedns.net"]
  local_auth_enabled                 = false
  identity {
    type = "SystemAssigned"
  }
}

############################################################################
# Create Azure Computer Vision
############################################################################
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
