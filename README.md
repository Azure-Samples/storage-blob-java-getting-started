---
services: storage
platforms: java
author: sribhat-MSFT
---

# Getting Started with Azure Blob Service in Java

Azure Blob Service Sample - Demonstrates how to perform common tasks using the Microsoft Azure Blob Service.

Blob storage stores unstructured data such as text, binary data, documents or media files. Blobs can be accessed from anywhere in the world via HTTP or HTTPS.

Note: If you don't have a Microsoft Azure subscription you can get a FREE trial account [here](http://go.microsoft.com/fwlink/?LinkId=330212)

## Running this sample

This sample can be run using either the Azure Storage Emulator, your Azure Storage account by updating the config.properties file with your "AccountName" and "Key", or using the Azure CLI.

To run the sample using the Storage Emulator (default option - Only available on Microsoft Windows OS):

1. Start the Azure Storage Emulator by pressing the Start button or the Windows key and searching for it by typing "Azure Storage Emulator". Select it from the list of applications to start it.
2.  Set breakpoints and run the project.

To run the sample using the Storage Service:

1. Open the config.properties file and comment out the connection string for the emulator "UseDevelopmentStorage=True" and uncomment the connection string for the storage service "AccountName=[]".
2. Create a Storage Account through the Azure Portal and provide your account name and account key in the config.properties file.
3. Set breakpoints and run the project.

To run the sample using Azure CLI:

1. [Install Azure CLI](https://azure.microsoft.com/en-us/documentation/articles/xplat-cli-install/)
2. [Login with Azure CLI](https://azure.microsoft.com/en-us/documentation/articles/xplat-cli-connect/)
3. Run node setup.js
4. mvn compile exec:java
5. Run node teardown.js

## More information

[What is a Storage Account](http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/)

[Getting Started with Blobs](http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-blob-storage/)

[Blob Service Concepts](http://msdn.microsoft.com/en-us/library/dd179376.aspx)

[Blob Service REST API](http://msdn.microsoft.com/en-us/library/dd135733.aspx)

[Blob Service Java API](http://azure.github.io/azure-storage-java/)

[Delegating Access with Shared Access Signatures](http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-shared-access-signature-part-1/)

[Storage Emulator](http://azure.microsoft.com/en-us/documentation/articles/storage-use-emulator/)

