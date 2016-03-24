
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
createAzureResourceGroup();

function createAzureResourceGroup() {
	var prc = spawn('azure', ['group', 'create', resourceGroup, location]);
	prc.stderr.on('data', (data) => {
		errorHasOccurred = true;
	  	process.stdout.write(`stderr: ${data}`);
	});

	prc.stdout.on('data', (data) => {
	  	process.stdout.write(`${data}`);
	});

	prc.on('close', (code) => {
		if(!errorHasOccurred) {
			createStorageAccount(); 
		}
	});
}

function createStorageAccount() {
	var prc = spawn('azure', ['storage', 'account', 'create', storageAccountName, '-g', resourceGroup, '--type', 'RAGRS', '-l', location]);
	prc.stderr.on('data', (data) => {
		errorHasOccurred = true;
	  	process.stdout.write(`stderr: ${data}`);
	});

	prc.stdout.on('data', (data) => {
	  	process.stdout.write(`${data}`);
	});

	prc.on('close', (code) => {
		if(!errorHasOccurred) {
			getStorageAccountConnStr(); 
		}
	});
}

function getStorageAccountConnStr() {
	var prc = spawn('azure', ['storage', 'account', 'connectionstring', 'show', '--json', storageAccountName, '-g', resourceGroup]);
	prc.stderr.on('data', (data) => {
		errorHasOccurred = true;
	  	console.log(`ps stderr: ${data}`);
	  	process.stdout.write(`stderr: ${data}`);
	});

	prc.stdout.on('data', (data) => {
	  	process.stdout.write(`${data}`);
		connectionData += `${data}`;
	});

	prc.on('close', (code) => {
		if(!errorHasOccurred) {
			writeConfigProperties(); 
		}
	});

}

function writeConfigProperties() {
	var connectionString = JSON.parse(connectionData).string;
	var stream = fs.createWriteStream("resources/config.properties");
	stream.once('open', function(fd) {
	  stream.write('#{"uuid":"' + uuidrg + '"}\n');
	  stream.write('#StorageConnectionString = UseDevelopmentStorage=true\n');
	  stream.write('StorageConnectionString = ' + connectionString + '\n');
	  stream.end();
	});
}
