var fs = require("fs");
var spawn = require('child_process').spawn;

readUUIDFromConfigFile();

function readUUIDFromConfigFile() {
fs.readFile('resources/config.properties', (err, data) => {
  if (err) throw err;
  var fileData = `${data}`;
  var firstLineIndex = fileData.indexOf("\n");
  if(firstLineIndex > 0) {
  	var uuidrg = JSON.parse(fileData.substring(1, firstLineIndex + 1)).uuid;
  	deleteResourceGroup(uuidrg);
  }else {
  	console.log('unable to teardown');
  }
});
}

function deleteResourceGroup(uuidrg) {
  var resourceGroup = "rg" + uuidrg.substring(0,19);
  var prc = spawn('azure', ['group', 'delete', '-q', resourceGroup]);
  prc.stderr.on('data', (data) => {
    errorHasOccurred = true;
    process.stdout.write(`stderr: ${data}`);
  });

  prc.stdout.on('data', (data) => {
    process.stdout.write(`${data}`);
  });
}