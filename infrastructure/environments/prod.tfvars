environment       = "prod"
environment_short = "prod"


budget_alert_config = {
  expected_cost = 1000
}

vnet_cidr_block             = "10.36.12.0/22"
subnet_cidr_block           = "10.36.12.0/24"
functionapp_cidr_block      = "10.36.13.0/24"
daily_log_cap               = 2
function_app_python_version = 3.13
storage_containers = [
  "redactiondata",
  "analytics"
]

worker_caps = {
  llm       = 32
  vision    = 32
  signature = 4
}
vision_requests_per_minute = 1000 # Vision S1, below the tier limit of 20 per second = 1200 per minute
llm_quota_allocation       = 0.9 # Nothing else uses the prod deployment, so keep 10% headroom
