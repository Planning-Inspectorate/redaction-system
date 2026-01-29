locals {
  location_short  = "uks"
  location        = "uksouth"
  service_name    = "redaction-system"
  resource_suffix = "${local.service_name}-${var.environment}-${local.location_short}"
  org             = "pins"
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
  service_bus_topics = toset(
    [
      "redaction_process_start",
      "redaction_process_complete"
    ]
  )
}