variable "subscription_id" {
  description = "The subscription to connect to"
  type        = string
}

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