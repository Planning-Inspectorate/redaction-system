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
