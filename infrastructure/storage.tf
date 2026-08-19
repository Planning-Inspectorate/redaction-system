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
  for_each = toset(var.storage_containers)

  name                  = each.key
  storage_account_id    = azurerm_storage_account.redaction_storage.id
  container_access_type = "private"
}