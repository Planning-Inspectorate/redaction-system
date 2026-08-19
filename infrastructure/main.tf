############################################################################
# Create resource groups
############################################################################
resource "azurerm_resource_group" "primary" {
  name     = "${local.org}-rg-${local.resource_suffix}"
  location = local.location
  tags     = local.tags
}

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
