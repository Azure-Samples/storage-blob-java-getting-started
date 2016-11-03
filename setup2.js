// This script was built for the Azure CLI 2.0 preview
// for more information: http://github.com/azure/azure-cli

var uuid = require('node-uuid');
var fs = require('fs');
var spawn = require('child_process').spawn;

var uuidrg = uuid.v4().toLowerCase().replace(/-/g, '');
var resourceGroup = "rg" + uuidrg.substring(0,19);
var storageAccountName = "sa" + uuidrg.substring(0,19);
var location = "westus";
var errorHasOccurred = false;
var connectionData = "";

// az resource group create -n $resourceGroup -l $location
executeAzureCLI(['resource','group','create', '-n', resourceGroup, '-l', location], function() {

	// az storage account create -l $location -g $resourceGroup -n $storageAccountName --sku Standard_RAGRS
	executeAzureCLI(['storage','account','create', '-g', resourceGroup, '-l', location
	, '-n', storageAccountName, '--sku', 'Standard_RAGRS' ], function()
	{
		//az storage account show-connection-string -g $resourceGroup -n $storageAccountName
		executeAzureCLI(['storage','account','show-connection-string', '-g', resourceGroup, '-n', storageAccountName, '--output', 'json'], function(code)
		{
			var connectionString = JSON.parse(connectionData).string;
			var stream = fs.createWriteStream("resources/config.properties");
			stream.once('open', function(fd) {
				stream.write('#{"uuid":"' + uuidrg + '"}\n');
				stream.write('#StorageConnectionString = UseDevelopmentStorage=true\n');
				stream.write('StorageConnectionString = ' + connectionString + '\n');
				stream.end();
			});
    		console.log('Setup complete.');
		}, function(data)
		{
			connectionData += data;
		});
	});
});

function executeAzureCLI(params, closeCallback, stdOutCallback) {
	var prc = spawn('az', params);
	prc.stderr.on('data', (data) => {
		errorHasOccurred = true;

	  	process.stdout.write(`stderr: ${data}`);
	});

	prc.stdout.on('data', (data) => {
  		var outData = `${data}`;
		process.stdout.write(outData);
	  	if(stdOutCallback) {
	  		stdOutCallback(outData);
	  	}
	});

	prc.on('close', (code) => {
		if(!errorHasOccurred && closeCallback) {
			closeCallback(code);
		}
	});
}
