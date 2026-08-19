############################################################################
# Create resource groups
############################################################################
resource "azurerm_resource_group" "primary" {
  name     = "${local.org}-rg-${local.resource_suffix}"
  location = local.location
  tags     = local.tags
}

############################################################################
# Create App Insights / Monitoring resources
############################################################################
resource "azurerm_log_analytics_workspace" "redaction_system" {
  name                = "${local.org}-log-${local.resource_suffix}"
  location            = local.location
  resource_group_name = azurerm_resource_group.primary.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days
  daily_quota_gb      = var.daily_log_cap

  tags = local.tags
}

resource "azurerm_application_insights" "redaction_system" {
  name                = "${local.org}-ai-${local.resource_suffix}"
  location            = local.location
  resource_group_name = azurerm_resource_group.primary.name
  application_type    = "other"
  retention_in_days   = var.log_retention_days
  workspace_id        = azurerm_log_analytics_workspace.redaction_system.id

  tags = local.tags
}

resource "azurerm_monitor_action_group" "redaction_tech" {
  name                = "pins-ag-redaction-tech-${var.environment}"
  resource_group_name = azurerm_resource_group.primary.name
  short_name          = "redaction" # needs to be under 12 characters
  tags                = local.tags

  # we set emails in the action groups in Azure Portal - to avoid needing to manage emails in terraform
  lifecycle {
    ignore_changes = [
      email_receiver
    ]
  }
}

# Log cap alert using scheduled query rules
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "log_cap" {
  count = var.environment == "prod" ? 1 : 0

  name         = "Log cap Alert"
  display_name = "Daily logging limit (${var.daily_log_cap}GB) reached for ${local.service_name} in PROD"
  description  = "Triggered when the log Data cap is reached."

  location            = local.location
  resource_group_name = azurerm_resource_group.primary.name
  scopes              = [azurerm_log_analytics_workspace.redaction_system.id]

  enabled                 = true
  auto_mitigation_enabled = false

  evaluation_frequency = "PT5M"
  window_duration      = "PT5M"

  criteria {
    query                   = <<-QUERY
      _LogOperation
      | where Category =~ "Ingestion" | where Detail contains "OverQuota"
      QUERY
    time_aggregation_method = "Count"
    threshold               = 0
    operator                = "GreaterThan"
  }

  severity = 2
  action {
    action_groups = [azurerm_monitor_action_group.redaction_tech.id]
  }
}

############################################################################
# Create Azure Open AI
############################################################################


############################################################################
# Service bus subscriptions
# The topics themselves are defined at https://github.com/Planning-Inspectorate/infrastructure-environments
# Each team that uses the redaction system must have a block like this for their own subscription
############################################################################
resource "azurerm_servicebus_subscription" "redaction_process_complete" {
  name               = "redaction-system"
  topic_id           = data.azurerm_servicebus_topic.redaction_process_complete.id
  max_delivery_count = 1
}

resource "azurerm_servicebus_subscription_rule" "redaction_process_complete" {
  name            = "subscription_rule"
  subscription_id = azurerm_servicebus_subscription.redaction_process_complete.id
  filter_type     = "CorrelationFilter"
  correlation_filter {
    label = "redaction-system" # Each team will have their requests labelled with an id representing their team
  }
}
