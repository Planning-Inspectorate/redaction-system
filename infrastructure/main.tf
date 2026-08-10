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

resource "azurerm_storage_account" "redaction_storage" {
  #checkov:skip=CKV_AZURE_33: Logging not implemented yet
  #checkov:skip=CKV2_AZURE_1: Customer Managed Keys not implemented
  #checkov:skip=CKV_AZURE_206: Replication not needed
  #checkov:skip=CKV2_AZURE_40: Enable key-based authentication to allow ADO to access the storage account
  name                             = "${local.org}stredaction${var.environment_short}${local.location_short}"
  resource_group_name              = azurerm_resource_group.primary.name
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
    last_access_time_enabled = true
    delete_retention_policy {
      days = var.storage_retention_days
    }
  }

  sas_policy {
    expiration_period = "01.12:00:00"
  }

  network_rules {
    default_action = "Deny"
    bypass         = ["AzureServices"] # Keep Azure platform services in scope
  }
}

resource "azurerm_storage_management_policy" "main" {
  storage_account_id = azurerm_storage_account.redaction_storage.id

  rule {
    name    = "Data retention policy"
    enabled = true
    filters {
      prefix_match = ["redactiondata"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cold_after_days_since_last_access_time_greater_than = 7
        delete_after_days_since_creation_greater_than               = 7
      }
    }
  }

  rule {
    name    = "Analytics data retention policy"
    enabled = true
    filters {
      prefix_match = ["analytics"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cold_after_days_since_last_access_time_greater_than    = 30
        tier_to_archive_after_days_since_last_tier_change_greater_than = 60
      }
    }
  }

  rule {
    name    = "Test data retention policy"
    enabled = true
    filters {
      prefix_match = ["test"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cold_after_days_since_last_access_time_greater_than = 1
        delete_after_days_since_creation_greater_than               = 1
      }
    }
  }
}

resource "azurerm_storage_container" "redaction_storage" {
  #checkov:skip=CKV2_AZURE_21: Not needed
  for_each = local.storage_containers

  name                  = each.key
  storage_account_id    = azurerm_storage_account.redaction_storage.id
  container_access_type = "private"
}

# The dev env fileshare was manually created - deleting the dev infra to resync is quite a long process (due to resource locks)
# there are higher priority things to focus on right now. This should be resynced when possible
resource "azurerm_storage_share" "function_app_processor" {
  name               = azurerm_linux_function_app.processor.name
  storage_account_id = azurerm_storage_account.redaction_storage.id
  quota              = 5120
}
resource "azurerm_storage_share" "function_app" {
  name               = azurerm_linux_function_app.receiver.name
  storage_account_id = azurerm_storage_account.redaction_storage.id
  quota              = 5120
}

############################################################################
# Create Azure Function App
############################################################################
# Note: We use separate ASPs for receiving/processing messages due to high CPU utilisation which throttles requests at high load
resource "azurerm_service_plan" "receiver" {
  #checkov:skip=CKV_AZURE_212: TODO: Limit reached in subscription
  #checkov:skip=CKV_AZURE_225: TODO: Limit reached in subscription
  #checkov:skip=CKV_AZURE_211: Chose a basic plan to keep costs low and because the function app is very lightweight
  name                = "${local.org}-asp-receiver-${local.resource_suffix}"
  resource_group_name = azurerm_resource_group.primary.name
  location            = local.location
  os_type             = "Linux"
  sku_name            = "B1"
  #worker_count           = 2
  #zone_balancing_enabled = true
}

resource "azurerm_service_plan" "processor" {
  #checkov:skip=CKV_AZURE_212: TODO: Limit reached in subscription
  #checkov:skip=CKV_AZURE_225: TODO: Limit reached in subscription
  name                = "${local.org}-asp-processor-${local.resource_suffix}"
  resource_group_name = azurerm_resource_group.primary.name
  location            = local.location
  os_type             = "Linux"
  sku_name            = "P2v3"
  #worker_count           = 2
  #zone_balancing_enabled = true
}


resource "azurerm_linux_function_app" "processor" {
  name                = "${local.org}-func-processor-${local.resource_suffix}"
  resource_group_name = azurerm_resource_group.primary.name
  location            = local.location

  storage_account_name          = azurerm_storage_account.redaction_storage.name
  storage_account_access_key    = azurerm_storage_account.redaction_storage.primary_access_key
  service_plan_id               = azurerm_service_plan.processor.id
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
    always_on = true
  }
  identity {
    type = "SystemAssigned"
  }
  app_settings = {
    "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING" = "DefaultEndpointsProtocol=https;AccountName=${azurerm_storage_account.redaction_storage.name};AccountKey=${azurerm_storage_account.redaction_storage.primary_access_key};EndpointSuffix=core.windows.net"
    "WEBSITE_CONTENTSHARE" : "${local.org}-func-processor-${local.resource_suffix}"
    "SCM_DO_BUILD_DURING_DEPLOYMENT"                = "true"
    "OPENAI_ENDPOINT"                               = azurerm_cognitive_account.open_ai.endpoint
    "AZURE_VISION_ENDPOINT"                         = azurerm_cognitive_account.computer_vision.endpoint
    "STORAGE_NAME"                                  = azurerm_storage_account.redaction_storage.name
    "APP_INSIGHTS_CONNECTION_STRING"                = azurerm_application_insights.redaction_system.connection_string
    "WEBSITE_CONTENTOVERVNET"                       = 1
    "AZURE_SERVICE_BUS_NAMESPACE"                   = data.azurerm_servicebus_namespace.backoffice.name
    "AZURE_SERVICE_BUS_NAMESPACE_CONNECTION_STRING" = data.azurerm_servicebus_namespace.backoffice.default_primary_connection_string
    "SIGNATURE_DETECTOR_ENDPOINT"                   = azurerm_linux_web_app.signature_detector.default_hostname
  }
}

resource "azurerm_linux_function_app" "receiver" {
  name                = "${local.org}-func-${local.resource_suffix}"
  resource_group_name = azurerm_resource_group.primary.name
  location            = local.location

  storage_account_name          = azurerm_storage_account.redaction_storage.name
  storage_account_access_key    = azurerm_storage_account.redaction_storage.primary_access_key
  service_plan_id               = azurerm_service_plan.receiver.id
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
    always_on = true
  }
  identity {
    type = "SystemAssigned"
  }
  app_settings = {
    "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING" = "DefaultEndpointsProtocol=https;AccountName=${azurerm_storage_account.redaction_storage.name};AccountKey=${azurerm_storage_account.redaction_storage.primary_access_key};EndpointSuffix=core.windows.net"
    "WEBSITE_CONTENTSHARE" : "${local.org}-func-${local.resource_suffix}"
    "SCM_DO_BUILD_DURING_DEPLOYMENT"                = "true"
    "WEBSITE_CONTENTOVERVNET"                       = 1
    "AZURE_SERVICE_BUS_NAMESPACE"                   = data.azurerm_servicebus_namespace.backoffice.name
    "AZURE_SERVICE_BUS_NAMESPACE_CONNECTION_STRING" = data.azurerm_servicebus_namespace.backoffice.default_primary_connection_string
  }
}

# Renamed to avoid confusion between function apps
moved {
  from = azurerm_linux_function_app.redaction_system
  to   = azurerm_linux_function_app.receiver
}

resource "azurerm_linux_web_app" "signature_detector" {
  #checkov:skip=CKV_AZURE_88: Azure Files not needed for Docker container app
  #checkov:skip=CKV_AZURE_13: Internal service behind private network, authentication handled at application level
  name                          = "${local.org}-app-signature-detector-${local.resource_suffix}"
  location                      = local.location
  resource_group_name           = azurerm_resource_group.primary.name
  service_plan_id               = azurerm_service_plan.processor.id
  client_certificate_enabled    = true
  https_only                    = true
  public_network_access_enabled = false
  virtual_network_subnet_id     = azurerm_subnet.function_app.id

  identity {
    type = "SystemAssigned"
  }

  logs {
    failed_request_tracing  = true
    detailed_error_messages = true

    http_logs {
      file_system {
        retention_in_days = 7
        retention_in_mb   = 35
      }
    }

  }


  site_config {
    always_on         = true
    http2_enabled     = true
    ftps_state        = "Disabled"
    health_check_path = "/health"

    application_stack {
      docker_image_name        = "signature-detector:latest"
      docker_registry_url      = "https://${azurerm_container_registry.container_registry.login_server}"
      docker_registry_username = ""
      docker_registry_password = ""
    }

    ip_restriction_default_action = "Allow"

  }

  lifecycle {
    ignore_changes = [
      site_config[0].application_stack[0].docker_image_name,
      tags
    ]
  }

  app_settings = {
    "WEBSITES_PORT"                       = "8080"
    "DOCKER_ENABLE_CI"                    = "true"
    "TRANSFORMERS_OFFLINE"                = "1"
    "HF_HUB_OFFLINE"                      = "1"
    "APP_INSIGHTS_CONNECTION_STRING"      = azurerm_application_insights.redaction_system.connection_string
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE" = "false"
    "DOCKER_REGISTRY_SERVER_URL"          = "https://${azurerm_container_registry.container_registry.login_server}"
  }

  tags = local.tags
}


############################################################################
# Create App Insights / Monitoring resources
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

resource "azurerm_monitor_action_group" "redaction_tech" {
  name                = "pins-ag-redaction-tech-${var.environment}"
  resource_group_name = azurerm_resource_group.primary.name
  short_name          = "redaction" # needs to be under 12 characters
  tags                = local.tags

  # we set emails in the action groups in Azure Portal - to avoid needing to manage emails in terraform
  lifecycle {
    ignore_changes = [
      email_receiver
    ]
  }
}

# Log cap alert using scheduled query rules
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "log_cap" {
  count = var.environment == "prod" ? 1 : 0

  name         = "Log cap Alert"
  display_name = "Daily logging limit (${var.daily_log_cap}GB) reached for ${local.service_name} in PROD"
  description  = "Triggered when the log Data cap is reached."

  location            = local.location
  resource_group_name = azurerm_resource_group.primary.name
  scopes              = [azurerm_log_analytics_workspace.redaction_system.id]

  enabled                 = true
  auto_mitigation_enabled = false

  evaluation_frequency = "PT5M"
  window_duration      = "PT5M"

  criteria {
    query                   = <<-QUERY
      _LogOperation
      | where Category =~ "Ingestion" | where Detail contains "OverQuota"
      QUERY
    time_aggregation_method = "Count"
    threshold               = 0
    operator                = "GreaterThan"
  }

  severity = 2
  action {
    action_groups = [azurerm_monitor_action_group.redaction_tech.id]
  }
}

resource "azurerm_monitor_diagnostic_setting" "web_app_logs" {
  name                       = "Web App Logs"
  log_analytics_workspace_id = azurerm_log_analytics_workspace.redaction_system.id
  target_resource_id         = azurerm_linux_web_app.signature_detector.id

  enabled_log {
    category = "AppServiceConsoleLogs"
  }

  lifecycle {
    ignore_changes = [
      enabled_log,
      metric
    ]
  }
}

############################################################################
# Create Azure Open AI
############################################################################
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

############################################################################
# Service bus subscriptions
# The topics themselves are defined at https://github.com/Planning-Inspectorate/infrastructure-environments
# Each team that uses the redaction system must have a block like this for their own subscription
############################################################################
resource "azurerm_servicebus_subscription" "redaction_process_complete" {
  name               = "redaction-system"
  topic_id           = data.azurerm_servicebus_topic.redaction_process_complete.id
  max_delivery_count = 1
}

resource "azurerm_servicebus_subscription_rule" "redaction_process_complete" {
  name            = "subscription_rule"
  subscription_id = azurerm_servicebus_subscription.redaction_process_complete.id
  filter_type     = "CorrelationFilter"
  correlation_filter {
    label = "redaction-system" # Each team will have their requests labelled with an id representing their team
  }
}

############################################################################
# Container Registry
############################################################################
resource "azurerm_container_registry" "container_registry" {
  #checkov:skip=CKV_AZURE_164: Ensures that ACR uses signed/trusted images
  #checkov:skip=CKV_AZURE_165: Georeplication not necessary
  #checkov:skip=CKV_AZURE_166: "Ensure container image quarantine, scan, and mark images verified"
  #checkov:skip=CKV_AZURE_167: "Ensure a retention policy is set to cleanup untagged manifests."
  #checkov:skip=CKV_AZURE_233: Zone redundancy not needed
  name                          = "${local.org}crredaction${var.environment_short}${local.location_short}"
  resource_group_name           = azurerm_resource_group.primary.name
  location                      = local.location
  admin_enabled                 = false
  sku                           = "Premium"
  public_network_access_enabled = false
  retention_policy_in_days      = 7
  data_endpoint_enabled         = true

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}