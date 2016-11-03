// This script was built for the Azure CLI 2.0 preview
// for more information: http://github.com/azure/azure-cli

var fs = require("fs");
var spawn = require('child_process').spawn;
var errorHasOccurred = false;
var connectionData = "";

readUUIDFromConfigFile();

function readUUIDFromConfigFile() {
  fs.readFile('resources/config.properties', (err, data) => {
    if (err) throw err;
    var fileData = `${data}`;
    var firstLineIndex = fileData.indexOf("\n");
    if(firstLineIndex > 0) {
      var uuidrg = JSON.parse(fileData.substring(1, firstLineIndex + 1)).uuid;
      var resourceGroup = "rg" + uuidrg.substring(0,19);

      executeAzureCLI(['resource', 'group', 'delete', '-n', resourceGroup], function() {
          var stream = fs.createWriteStream("resources/config.properties"); 
          stream.once('open', function(fd) {
              stream.write('#By default we are assuming you will use the Azure Storage Emulator. If you have an Azure Subscription, you can alternatively\n');
              stream.write('#create a Storage Account and run against the storage service by commenting out the connection string below and using the\n'); 
              stream.write('#second connection string - in which case you must also insert your storage account name and key in the line below.\n\n');
              stream.write('StorageConnectionString = UseDevelopmentStorage=true\n');
              stream.write('#StorageConnectionString = DefaultEndpointsProtocol=https;AccountName=[AccountName];AccountKey=[AccountKey]\n');
              stream.end();
              console.log('Teardown complete.');
          });
      });
    }else {
      console.log('unable to teardown');
    }
  });
}

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
