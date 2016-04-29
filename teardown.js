var fs = require("fs");
var spawn = require('child_process').spawn;
var wasInArmMode = false;

readUUIDFromConfigFile();
var errorHasOccurred = false;
var connectionData = "";
var wasInArmMode = false;

function readUUIDFromConfigFile() {
  fs.readFile('resources/config.properties', (err, data) => {
    if (err) throw err;
    var fileData = `${data}`;
    var firstLineIndex = fileData.indexOf("\n");
    if(firstLineIndex > 0) {
      var uuidrg = JSON.parse(fileData.substring(1, firstLineIndex + 1)).uuid;
      executeAzureCLI(['config', 'list'], function() {
        executeAzureCLI(['config', 'mode', 'arm'], function() {
          var resourceGroup = "rg" + uuidrg.substring(0,19);
          executeAzureCLI(['group', 'delete', '-q', resourceGroup], function() {
              var stream = fs.createWriteStream("resources/config.properties");
              stream.once('open', function(fd) {
                stream.write('#By default we are assuming you will use the Azure Storage Emulator. If you have an Azure Subscription, you can alternatively\n');
                stream.write('#create a Storage Account and run against the storage service by commenting out the connection string below and using the\n'); 
                stream.write('#second connection string - in which case you must also insert your storage account name and key in the line below.\n\n');
                stream.write('StorageConnectionString = UseDevelopmentStorage=true\n');
                stream.write('#StorageConnectionString = DefaultEndpointsProtocol=https;AccountName=[AccountName];AccountKey=[AccountKey]\n');
                stream.end();
              });

              if(!wasInArmMode) {
                executeAzureCLI(['config', 'mode', 'asm']);
              }
              });
          });
      }, function(data) {
        wasInArmMode = data.match(/mode\s*arm/) != null     
      });

    }else {
      console.log('unable to teardown');
    }
  });
}

function executeAzureCLI(params, closeCallback, stdOutCallback) {
  var prc = spawn('azure', params);
  prc.stderr.on('data', (data) => {
    errorHasOccurred = true;
    if(!wasInArmMode && !("config mode asm" + params.join(' '))) {
      executeAzureCLI(['config', 'mode', 'asm']);
    }

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
