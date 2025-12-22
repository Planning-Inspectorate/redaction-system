locals {
  location     = "uks"
  service_name = "redaction"
  tags = {
    "CreatedBy" : "",
    "Environment" : var.environment,
    "ServiceName" : local.service_name
  }
  storage_containers = toset(
    [

    ]
  )
}