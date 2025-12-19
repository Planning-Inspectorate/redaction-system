locals {
  location     = "uks"
  service_name = "redaction"
  tags = {
    "CreatedBy" : "",
    "Environment" : var.environment,
    "ServiceName" : local.service_name
  }
  data_lake_storage_containers = toset(
    [

    ]
  )
}