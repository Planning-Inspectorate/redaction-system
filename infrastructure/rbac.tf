############################################################################
# Service permissions
############################################################################


resource "azurerm_role_assignment" "function_app_servicebus_datasender" {
  scope                = data.azurerm_servicebus_namespace.backoffice.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = azurerm_linux_function_app.receiver.identity[0].principal_id
}

############################################################################
# Engineer permissions
############################################################################

resource "azurerm_role_assignment" "engineer_storage_contributor" {
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_group.redaction_engineers.object_id
}

resource "azurerm_role_assignment" "engineer_storage_queue_contributor" {
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = data.azuread_group.redaction_engineers.object_id
}

resource "azurerm_role_assignment" "engineer_redaction_resource_group_contributor" {
  scope                = azurerm_resource_group.primary.id
  role_definition_name = "Contributor"
  principal_id         = data.azuread_group.redaction_engineers.object_id
}

resource "azurerm_role_assignment" "engineer_openai_contributor" {
  scope                = azurerm_cognitive_account.open_ai.id
  role_definition_name = "Cognitive Services OpenAI User"
  principal_id         = data.azuread_group.redaction_engineers.object_id
}

resource "azurerm_role_assignment" "engineer_computervision_contributor" {
  scope                = azurerm_cognitive_account.computer_vision.id
  role_definition_name = "Cognitive Services User"
  principal_id         = data.azuread_group.redaction_engineers.object_id
}

resource "azurerm_role_assignment" "engineer_servicebus_dataowner" {
  scope                = data.azurerm_servicebus_namespace.backoffice.id
  role_definition_name = "Azure Service Bus Data Owner"
  principal_id         = data.azuread_group.redaction_engineers.object_id
}

############################################################################
# ADO permissions (for integration tests)
############################################################################
resource "azurerm_role_assignment" "ado_deployment_storage_contributor" {
  count                = var.environment != "prod" ? 1 : 0
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_service_principal.deployment.object_id
}

resource "azurerm_role_assignment" "ado_deployment_functions_processor_contributor" {
  scope                = azurerm_linux_function_app.processor.id
  role_definition_name = "Contributor"
  principal_id         = data.azuread_service_principal.deployment.object_id
}

resource "azurerm_role_assignment" "ado_deployment_functions_contributor" {
  scope                = azurerm_linux_function_app.receiver.id
  role_definition_name = "Contributor"
  principal_id         = data.azuread_service_principal.deployment.object_id
}

resource "azurerm_role_assignment" "ado_deployment_signature_detector_contributor" {
  scope                = azurerm_linux_web_app.signature_detector.id
  role_definition_name = "Contributor"
  principal_id         = data.azuread_service_principal.deployment.object_id
}

resource "azurerm_role_assignment" "ado_deployment_functions_container_registry_acr_pull" {
  scope                = data.azurerm_container_registry.container_registry.id
  role_definition_name = "AcrPull"
  principal_id         = data.azuread_service_principal.deployment.object_id
}

resource "azurerm_role_assignment" "ado_deployment_functions_container_registry_acr_push" {
  scope                = data.azurerm_container_registry.container_registry.id
  role_definition_name = "AcrPush"
  principal_id         = data.azuread_service_principal.deployment.object_id
}

resource "azurerm_role_assignment" "ado_ci_storage_contributor" {
  count                = var.environment != "prod" ? 1 : 0
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_service_principal.ci.object_id
}

resource "azurerm_role_assignment" "ado_openai_contributor" {
  count                = var.environment != "prod" ? 1 : 0
  scope                = azurerm_cognitive_account.open_ai.id
  role_definition_name = "Cognitive Services OpenAI User"
  principal_id         = data.azuread_service_principal.ci.object_id
}

resource "azurerm_role_assignment" "ado_computervision_contributor" {
  count                = var.environment != "prod" ? 1 : 0
  scope                = azurerm_cognitive_account.computer_vision.id
  role_definition_name = "Cognitive Services User"
  principal_id         = data.azuread_service_principal.ci.object_id
}

resource "azurerm_role_assignment" "ado_servicebus_datasender" {
  count                = var.environment != "prod" ? 1 : 0
  scope                = data.azurerm_servicebus_namespace.backoffice.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = data.azuread_service_principal.ci.object_id
}

resource "azurerm_role_assignment" "ado_servicebus_datareceiver" {
  count                = var.environment != "prod" ? 1 : 0
  scope                = data.azurerm_servicebus_namespace.backoffice.id
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = data.azuread_service_principal.ci.object_id
}
