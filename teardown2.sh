
# This script was built for the Azure CLI 2.0 preview
# for more information: http://github.com/azure/azure-cli

uuid=$(head -n 1 resources/config.properties)
resourceGroup="rg${uuid:1:20}"
echo "Deleting resource group: $resourceGroup..."
az resource group delete -n $resourceGroup