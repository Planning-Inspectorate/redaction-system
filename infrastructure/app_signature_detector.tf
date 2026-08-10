resource "azurerm_linux_web_app" "signature_detector" {
  #checkov:skip=CKV_AZURE_13: Internal service behind private network, authentication handled at application level
  #checkov:skip=CKV_AZURE_17: Internal service behind private network, client certificates not required
  #checkov:skip=CKV_AZURE_88: Azure Files not needed for Docker container app
  name                          = "${local.org}-app-signature-detector-${local.resource_suffix}"
  location                      = local.location
  resource_group_name           = azurerm_resource_group.primary.name
  service_plan_id               = azurerm_service_plan.processor.id
  client_certificate_enabled    = false
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
    always_on                               = true
    container_registry_use_managed_identity = true
    ftps_state                              = "Disabled"
    health_check_path                       = "/health"
    health_check_eviction_time_in_min       = 10
    http2_enabled                           = true

    application_stack {
      docker_image_name   = "redaction/signature-detector:latest"
      docker_registry_url = "https://${data.azurerm_container_registry.container_registry.login_server}"
    }

    ip_restriction_default_action = "Deny"
  }

  app_settings = {
    WEBSITES_PORT                       = "8080"
    DOCKER_ENABLE_CI                    = "true"
    TRANSFORMERS_OFFLINE                = "1"
    HF_HUB_OFFLINE                      = "1"
    APP_INSIGHTS_CONNECTION_STRING      = azurerm_application_insights.redaction_system.connection_string
    WEBSITES_ENABLE_APP_SERVICE_STORAGE = "false"
  }

  tags = local.tags

  lifecycle {
    ignore_changes = [
      # ignore any changes to the docker image, since the image tag changes per deployment
      # all other site_config and application_stack changes should be tracked
      # see state file to check structure: site_config and application_stack are arrays in state, with a single entry
      site_config[0].application_stack[0].docker_image_name,
      tags
    ]
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

# private endpoint
resource "azurerm_private_endpoint" "signature_detector" {
  name                = "${local.org}-pe-${azurerm_linux_web_app.signature_detector.name}-${var.environment}"
  resource_group_name = azurerm_resource_group.primary.name
  location            = local.location
  subnet_id           = azurerm_subnet.redaction_system.id
  private_dns_zone_group {
    name                 = "${local.org}-pdns-${local.service_name}-webapp-signature-detector-${var.environment}"
    private_dns_zone_ids = [data.azurerm_private_dns_zone.function.id]
  }

  private_service_connection {
    name                           = "${local.org}-psc-${local.service_name}-webapp-signature-detector-${var.environment}"
    is_manual_connection           = false
    private_connection_resource_id = azurerm_linux_web_app.signature_detector.id
    subresource_names              = ["sites"]
  }

  tags = local.tags
}

# RBAC for blob access
resource "azurerm_role_assignment" "signature_detector_storage_contributor" {
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_web_app.signature_detector.identity[0].principal_id
}

# RBAC for container registry access to pull images
resource "azurerm_role_assignment" "signature_detector_acr_pull" {
  scope                = data.azurerm_container_registry.container_registry.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_linux_web_app.signature_detector.identity[0].principal_id
}