environment                 = "prod"
vnet_cidr_block             = "10.36.8.0/22"
subnet_cidr_block           = "10.36.8.0/24"
functionapp_cidr_block      = "10.36.9.0/24"
daily_log_cap               = 1
function_app_python_version = 3.13
storage_containers = [
  "redactiondata"
]
service_bus_premium_enabled = true