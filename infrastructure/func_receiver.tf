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

resource "azurerm_storage_share" "function_app" {
  name               = azurerm_linux_function_app.receiver.name
  storage_account_id = azurerm_storage_account.redaction_storage.id
  quota              = 5120
}

############################################################################
# RBAC
############################################################################

resource "azurerm_role_assignment" "function_app_servicebus_datasender" {
  scope                = data.azurerm_servicebus_namespace.backoffice.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = azurerm_linux_function_app.receiver.identity[0].principal_id
}