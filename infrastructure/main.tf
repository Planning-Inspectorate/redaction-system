# data "azurerm_client_config" "current" {}

# data "azurerm_subscription" "current" {}

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
  #checkov:skip=CKV2_AZURE_33: Private endpoint TBA
  name                             = "pinsst${local.service_name}${var.environment}${local.location_short}"
  resource_group_name              = azurerm_resource_group.redaction_rg.name
  location                         = local.location
  account_tier                     = "Standard"
  account_replication_type         = var.storage_account_replication_type
  account_kind                     = "StorageV2"
  min_tls_version                  = "TLS1_2"
  allow_nested_items_to_be_public  = "false"
  cross_tenant_replication_enabled = "false"
  shared_access_key_enabled        = false
  default_to_oauth_authentication  = true
  public_network_access_enabled    = true
  tags                             = local.tags

  blob_properties {
    delete_retention_policy {
      days = var.storage_retention_days
    }
  }

  lifecycle {
    prevent_destroy = true
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