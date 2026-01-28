############################################################################
# Service permissions
############################################################################
resource "azurerm_role_assignment" "function_app_storage_contributor" {
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_function_app.redaction_system.identity[0].principal_id
}

resource "azurerm_role_assignment" "function_app_openai_contributor" {
  scope                = azurerm_cognitive_account.open_ai.id
  role_definition_name = "Cognitive Services OpenAI User"
  principal_id         = azurerm_linux_function_app.redaction_system.identity[0].principal_id
}

resource "azurerm_role_assignment" "function_app_computervision_contributor" {
  scope                = azurerm_cognitive_account.computer_vision.id
  role_definition_name = "Cognitive Services User"
  principal_id         = azurerm_linux_function_app.redaction_system.identity[0].principal_id
}

############################################################################
# Engineer permissions
############################################################################
resource "azurerm_role_assignment" "engineer_storage_contributor" {
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
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

############################################################################
# ADO permissions (for integration tests)
############################################################################
resource "azurerm_role_assignment" "ado_deployment_storage_contributor" {
  count                = var.environment != "prod" ? 1 : 0
  scope                = azurerm_storage_account.redaction_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_service_principal.deployment.object_id
}

resource "azurerm_role_assignment" "ado_deployment_functions_contributor" {
  scope                = azurerm_linux_function_app.redaction_system.id
  role_definition_name = "Contributor"
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
