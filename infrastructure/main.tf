#data "azurerm_client_config" "current" {}

# data "azurerm_subscription" "current" {}

############################################################################
# User groups
############################################################################
data "azuread_group" "redaction_engineers" {
  display_name = "pins-redaction-system-developers"
  #security_enabled = true
}
data "azuread_service_principal" "deployment" {
  display_name = "Azure DevOps Pipelines - Redaction System - Deployment ${upper(var.environment)}"
}

data "azuread_service_principal" "ci" {
  display_name = "Azure DevOps Pipelines - Redaction System CI/CD"
}

############################################################################
# Create resource groups
############################################################################
resource "azurerm_resource_group" "redaction_rg" {
  name     = "pins-rg-${local.service_name}-${var.environment}-${local.location_short}"
  location = local.location
  tags     = local.tags
}

############################################################################
# Create storage account
############################################################################

resource "azurerm_storage_account" "redaction_storage" {
  #checkov:skip=CKV_AZURE_33: Logging not implemented yet
  #checkov:skip=CKV2_AZURE_1: Customer Managed Keys not implemented
  #checkov:skip=CKV_AZURE_206: Replication not needed
  name                             = "pinsst${local.service_name}${var.environment}${local.location_short}"
  resource_group_name              = azurerm_resource_group.redaction_rg.name
  location                         = local.location
  account_tier                     = "Standard"
  account_replication_type         = var.storage_account_replication_type
  account_kind                     = "StorageV2"
  min_tls_version                  = "TLS1_2"
  allow_nested_items_to_be_public  = "false"
  cross_tenant_replication_enabled = "false"
  shared_access_key_enabled        = true
  default_to_oauth_authentication  = true
  public_network_access_enabled    = false
  https_traffic_only_enabled       = true
  tags                             = local.tags

  blob_properties {
    delete_retention_policy {
      days = var.storage_retention_days
    }
  }

  lifecycle {
    prevent_destroy = false
  }

  sas_policy {
    expiration_period = "01.12:00:00"
  }

  network_rules {
    default_action = "Deny"
    bypass         = ["AzureServices"] # Keep Azure platform services in scope
  }
}

resource "azurerm_storage_container" "redaction_storage" {
  #checkov:skip=CKV2_AZURE_21: Implemented in synapse-monitoring module
  for_each = local.storage_containers

  name                  = each.key
  storage_account_name  = azurerm_storage_account.redaction_storage.name
  container_access_type = "private"
}

############################################################################
# Create Azure Function App
############################################################################

resource "azurerm_service_plan" "redaction_system" {
  #checkov:skip=CKV_AZURE_212: TODO: Limit reached in subscription
  #checkov:skip=CKV_AZURE_225: TODO: Limit reached in subscription
  name                = "pins-redaction-system-${var.environment}-${local.location_short}"
  resource_group_name = azurerm_resource_group.redaction_rg.name
  location            = local.location
  os_type             = "Linux"
  sku_name            = "EP1"
  #worker_count           = 2
  #zone_balancing_enabled = true
}

resource "azurerm_linux_function_app" "redaction_system" {
  name                = "pins-func-redaction-system-${var.environment}-${local.location_short}"
  resource_group_name = azurerm_resource_group.redaction_rg.name
  location            = local.location

  storage_account_name          = azurerm_storage_account.redaction_storage.name
  storage_account_access_key    = azurerm_storage_account.redaction_storage.primary_access_key
  service_plan_id               = azurerm_service_plan.redaction_system.id
  public_network_access_enabled = false
  virtual_network_subnet_id     = azurerm_subnet.function_app.id
  https_only                    = true

  site_config {
    application_stack {
      python_version = 3.13
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
    "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING" = "DefaultEndpointsProtocol=https;AccountName=${azurerm_storage_account.redaction_storage.name};AccountKey=${azurerm_storage_account.redaction_storage.primary_access_key};EndpointSuffix=core.windows.net"
    "SCM_DO_BUILD_DURING_DEPLOYMENT"           = "true"
    "OPENAI_ENDPOINT"                          = azurerm_cognitive_account.open_ai.endpoint
    "OPENAI_KEY"                               = azurerm_cognitive_account.open_ai.primary_access_key
    "AZURE_VISION_ENDPOINT"                    = azurerm_cognitive_account.computer_vision.endpoint
    "AZURE_VISION_KEY"                         = azurerm_cognitive_account.computer_vision.primary_access_key
    "APP_INSIGHTS_CONNECTION_STRING"           = azurerm_application_insights.redaction_system.connection_string
    "WEBSITE_CONTENTOVERVNET"                  = 1
  }
}

############################################################################
# Create App Insights
############################################################################
resource "azurerm_log_analytics_workspace" "redaction_system" {
  name                = "pins-log-redaction-system-${var.environment}-${local.location_short}"
  location            = local.location
  resource_group_name = azurerm_resource_group.redaction_rg.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days
  daily_quota_gb      = var.daily_log_cap

  tags = local.tags
}

resource "azurerm_application_insights" "redaction_system" {
  name                = "pins-ai-redaction-system-${var.environment}-${local.location_short}"
  location            = local.location
  resource_group_name = azurerm_resource_group.redaction_rg.name
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
  name                               = "pins-openai-redaction-system-${var.environment}-${local.location_short}"
  location                           = local.location
  resource_group_name                = azurerm_resource_group.redaction_rg.name
  kind                               = "OpenAI"
  sku_name                           = "S0"
  custom_subdomain_name              = "pins-redaction-openai-${var.environment}-${local.location_short}"
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
  name                               = "pins-cv-redaction-system-${var.environment}-${local.location_short}"
  location                           = local.location
  resource_group_name                = azurerm_resource_group.redaction_rg.name
  kind                               = "ComputerVision"
  sku_name                           = "F0"
  custom_subdomain_name              = "pins-redaction-computervision-${var.environment}-${local.location_short}"
  public_network_access_enabled      = false
  outbound_network_access_restricted = true
  fqdns                              = ["azureprivatedns.net"]
  local_auth_enabled                 = false
  identity {
    type = "SystemAssigned"
  }
}

############################################################################
# Create Role Assignments
############################################################################
resource "azurerm_role_assignment" "function_app_storage_contributor" {
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_function_app.redaction_system.identity[0].principal_id
}

resource "azurerm_role_assignment" "engineer_storage_contributor" {
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_group.redaction_engineers.object_id
}

resource "azurerm_role_assignment" "engineer_redaction_resource_group_contributor" {
  scope                = azurerm_resource_group.redaction_rg.id
  role_definition_name = "Contributor"
  principal_id         = data.azuread_group.redaction_engineers.object_id
}

resource "azurerm_role_assignment" "ado_deployment_storage_contributor" {
  count                = var.environment != "prod" ? 1 : 0
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_service_principal.deployment.object_id
}


resource "azurerm_role_assignment" "ado_ci_storage_contributor" {
  count                = var.environment != "prod" ? 1 : 0
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_service_principal.ci.object_id
}
