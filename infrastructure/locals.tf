locals {
  location_short = "uks"
  location       = "uksouth"
  service_name   = "redaction"
  tags = {
    "CreatedBy" : "",
    "Environment" : var.environment,
    "ServiceName" : local.service_name
  }
  storage_containers = toset(
    [

    ]
  )
  storage_subresources = [
    "blob",
    "queue",  # Needed for Azure Durable functions
    "table",
    "file"
  ]
  tooling_config = {
    network_name    = "pins-vnet-shared-tooling-uks"
    network_rg      = "pins-rg-shared-tooling-uks"
    subscription_id = "edb1ff78-90da-4901-a497-7e79f966f8e2"
  }
}