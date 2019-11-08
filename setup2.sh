
# This script was built for the Azure CLI 2.0 preview
# for more information: http://github.com/azure/azure-cli

uuidrg=$(uuidgen)
uuidrg=${uuidrg//-/}
uuidrg="$(tr '[:upper:]' '[:lower:]' <<< "$uuidrg")"
resourceGroup="rg${uuidrg:0:20}"
storageAccountName="sa${uuidrg:0:20}"
location="westus"

az resource group create -n $resourceGroup -l $location
az storage account create -l $location -g $resourceGroup -n $storageAccountName --sku Standard_RAGRS
connStr=$(az storage account show-connection-string -g $resourceGroup -n $storageAccountName --out tsv)
echo "#$uuidrg" > config.properties
echo '#StorageConnectionString = UseDevelopmentStorage=true' >> config.properties
echo "StorageConnectionString=${connStr:11}" >> config.properties
mv config.properties resources/
echo "Created config.properties files and placed in ./resources"
