locals {
  location_short     = "uks"
  location           = "uksouth"
  ai_location_short  = "weu"
  ai_location        = "westeurope"
  service_name       = "redaction-system"
  resource_suffix    = "${local.service_name}-${var.environment}-${local.location_short}"
  ai_resource_suffix = "${local.service_name}-${var.environment}-${local.ai_location_short}"
  org                = "pins"
  tags = {
    "CreatedBy" : "terraform",
    "Environment" : var.environment,
    "ServiceName" : local.service_name
    "Location" : local.location
  }
  storage_containers = toset(var.storage_containers)
  storage_subresources = [
    "blob",
    "queue", # Needed for Azure Durable functions
    "table",
    "file"
  ]
}