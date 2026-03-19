resource "azurerm_consumption_budget_resource_group" "ai_cost" {
  name              = "${local.org}-budget-${local.resource_suffix}"
  resource_group_id = azurerm_resource_group.primary.id

  amount     = var.budget_alert_config.expected_cost
  time_grain = "Monthly"

  time_period {
    start_date = "2026-03-01T00:00:00Z"
  }

  filter {
    dimension {
      name = "ResourceType"
      values = [
        "microsoft.cognitiveservices/accounts"
      ]
    }
  }

  dynamic "notification" {
    for_each = toset(local.budget_alert_threshold_percentages)
    content {
      enabled        = true
      threshold      = notification.value
      operator       = "GreaterThanOrEqualTo"
      threshold_type = "Actual"

      contact_groups = [
        azurerm_monitor_action_group.redaction_tech.id
      ]
    }
  }
}

locals {
  budget_alert_threshold_percentages = [
    25,
    50,
    75,
    100,
    125,
    150
  ]
}