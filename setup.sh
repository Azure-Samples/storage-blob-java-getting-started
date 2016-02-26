
azure config mode arm
uuidrg=$(uuidgen)
uuidrg=${uuidrg//-/}
uuidrg="$(tr '[:upper:]' '[:lower:]' <<< "$uuidrg")"
resourceGroup="rg${uuidrg:0:20}"
storageAccountName="sa${uuidrg:0:20}"
location="westus"
azure group create -n $resourceGroup -l $location
azure storage account create $storageAccountName -g $resourceGroup --type RAGRS -l $location
azure storage account connectionstring show --json $storageAccountName -g $resourceGroup > connectionstring.json
connStr=$(grep -o '"string": "[^"]*' connectionstring.json)
echo "#$uuidrg" > config.properties
echo '#StorageConnectionString = UseDevelopmentStorage=true' >> config.properties
echo "StorageConnectionString=${connStr:11}" >> config.properties
mv config.properties resources/
