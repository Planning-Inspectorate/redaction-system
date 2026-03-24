environment = "dev"

budget_alert_config = {
  expected_cost = 80
}

vnet_cidr_block             = "10.36.0.0/22"
subnet_cidr_block           = "10.36.0.0/24"
functionapp_cidr_block      = "10.36.1.0/24"
function_app_python_version = 3.13
storage_containers = [
  "redactiondata",
  "test",
  "analytics"
]