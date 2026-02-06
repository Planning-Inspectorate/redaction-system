variable "environment" {
  description = "The environment to deploy to"
  type        = string
}

variable "storage_account_replication_type" {
  description = "The storage redundancy setting"
  type        = string
  default     = "LRS"
}

variable "storage_retention_days" {
  description = "The storage data retention period"
  type        = number
  default     = 7
}

variable "log_retention_days" {
  description = "The logging data retention period"
  type        = number
  default     = 30
}

variable "daily_log_cap" {
  description = "The max amount of logging data that can be logged in a day"
  type        = number
  default     = 0.2
}

variable "tooling_config" {
  description = "Config for the tooling subscription resources"
  type = object({
    network_name    = string
    network_rg      = string
    subscription_id = string
  })
}

variable "vnet_cidr_block" {
  description = "The CIDR range for the vnet"
  type        = string
}

variable "subnet_cidr_block" {
  description = "The CIDR range for the subnet"
  type        = string
}

variable "functionapp_cidr_block" {
  description = "The CIDR range for the function app's subnet"
  type        = string
}

variable "function_app_python_version" {
  description = "The python version to use in the function app"
  type        = number
}

variable "storage_containers" {
  description = "The containers to create in the main storage account"
  type        = list(string)
}

variable "openai_quota" {
  description = "The quota allocation for the Open AI deployment, which equates to x thousand tokens per minute"
  type        = number
  default     = 1000
}

variable "service_bus_premium_enabled" {
  description = "If the servcie bus should have the premium SKU"
  type        = bool
  default     = false
}
