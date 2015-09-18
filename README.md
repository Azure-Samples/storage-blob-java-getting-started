---
services: storage
platforms: java
author: micurd
---

# Azure Storage: Blobs

Demonstrates how to use the Blob Storage service. Blob storage stores unstructured data such as text, binary data, documents or media files. Blobs can be accessed from anywhere in the world via HTTP or HTTPS.

Note: If you don't have a Microsoft Azure subscription you can get a FREE trial account [here](http://go.microsoft.com/fwlink/?LinkId=330212)

## Running this sample

This sample can be run using either the Azure Storage Emulator (Windows) or by updating the config.properties file with your Storage account name and key.

To run the sample using the Storage Emulator (default option):

1. Download and install the Azure Storage Emulator https://azure.microsoft.com/en-us/downloads/ 
2. Start the emulator (once only) by pressing the Start button or the Windows key and searching for it by typing "Azure Storage Emulator". Select it from the list of applications to start it.
3. Set breakpoints and run the project. 

To run the sample using the Storage Service

1. Open the config.properties file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and uncomment the connection string for the storage service (AccountName=[]...)
2. Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in the config.properties file. See https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/ for more information
3. Set breakpoints and run the project. 

## More information
- [What is a Storage Account](http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/)
- [Getting Started with Blobs](http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-blob-storage/)
- [Blob Service Concepts](http://msdn.microsoft.com/en-us/library/dd179376.aspx)
- [Blob Service REST API](http://msdn.microsoft.com/en-us/library/dd135733.aspx)
- [Blob Service Java API](http://azure.github.io/azure-storage-java/)
- [Delegating Access with Shared Access Signatures](http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-shared-access-signature-part-1/)
- [Storage Emulator](http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx)