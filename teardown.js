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
