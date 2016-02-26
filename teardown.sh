uuid=$(head -n 1 resources/config.properties)
resourceGroup="rg${uuid:1:20}"
azure group delete -q $resourceGroup