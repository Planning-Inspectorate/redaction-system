############################################################################
# Create resource groups
############################################################################
resource "azurerm_resource_group" "primary" {
  name     = "${local.org}-rg-${local.resource_suffix}"
  location = local.location
  tags     = local.tags
}
