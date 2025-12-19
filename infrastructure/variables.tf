variable "environment" {
  description = "The environment to deploy to"
  type        = string
}

variable "storage_account_replication_type" {
  description = "The storage redundancy setting"
  type        = string
  default     = "GRS"
}

variable "data_lake_retention_days" {
  description = "The storage data retention period"
  type        = number
  default     = 7
}

variable "tooling_config" {
  description = "Config for the tooling subscription resources"
  type = object({
    container_registry_name = string
    container_registry_rg   = string
    network_name            = string
    network_rg              = string
    subscription_id         = string
  })
}
