
//azure config mode arm
var uuid = require('node-uuid');
var fs = require('fs');
var spawn = require('child_process').spawn;

var uuidrg = uuid.v4().toLowerCase().replace(/-/g, '');
var resourceGroup = "rg" + uuidrg.substring(0,19);
var storageAccountName = "sa" + uuidrg.substring(0,19);
var location = "westus";
var errorHasOccurred = false;
var connectionData = "";

executeAzureCLI(['group', 'create', resourceGroup, location], function() {
	executeAzureCLI(['storage', 'account', 'create', storageAccountName, '-g', resourceGroup, '--type', 'RAGRS', '-l', location], function(code) {
		executeAzureCLI(['storage', 'account', 'connectionstring', 'show', '--json', storageAccountName, '-g', resourceGroup], function(code) {
			var connectionString = JSON.parse(connectionData).string;
			var stream = fs.createWriteStream("resources/config.properties");
			stream.once('open', function(fd) {
			  stream.write('#{"uuid":"' + uuidrg + '"}\n');
			  stream.write('#StorageConnectionString = UseDevelopmentStorage=true\n');
			  stream.write('StorageConnectionString = ' + connectionString + '\n');
			  stream.end();
			});
		},
		function(data) {
			console.log('connectionData: ' + connectionData);
			connectionData += data;
		});
	});
});

function executeAzureCLI(params, closeCallback, stdOutCallback) {
	var prc = spawn('azure', params);
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
