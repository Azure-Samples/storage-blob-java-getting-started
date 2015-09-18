/**
 * Copyright Microsoft Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package azure_storage.storage_blob_java_getting_started;

import java.io.*;
import java.util.Properties;
import java.util.UUID;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

/**
* Azure Storage Blob Sample - Demonstrate how to use the Blob Storage service. 
* Blob storage stores unstructured data such as text, binary data, documents or media files. 
* Blobs can be accessed from anywhere in the world via HTTP or HTTPS.
* 
* Documentation References: 
*  - What is a Storage Account - http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/
*  - Getting Started with Blobs - http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-blob-storage/
*  - Blob Service Concepts - http://msdn.microsoft.com/en-us/library/dd179376.aspx 
*  - Blob Service REST API - http://msdn.microsoft.com/en-us/library/dd135733.aspx
*  - Blob Service Java API - http://azure.github.io/azure-storage-java/
*  - Delegating Access with Shared Access Signatures - http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-shared-access-signature-part-1/
*  - Storage Emulator - http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx
*/

public class App {
	/**
    * Instructions: This sample can be run using either the Azure Storage Emulator (Windows) or by  
    * updating the config.properties file with your Storage account name and key.
	*
    * To run the sample using the Azure Storage Emulator (default option)
    *      1. Download the Azure Storage Emulator https://azure.microsoft.com/en-us/downloads/ 
	*	   2. Start the emulator (once only) by pressing the Start button or the Windows key and searching for it
    *         by typing "Azure Storage Emulator". Select it from the list of applications to start it.
    *      3. Set breakpoints and run the project. 
    * 
    * To run the sample using the Storage Service
    *      1. Open the config.properties file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and
    *         uncomment the connection string for the storage service (AccountName=[]...)
    *      2. Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in 
    *         the config.properties file. See https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/ for more information
    *      3. Set breakpoints and run the project. 
    * 
    */

	public static void main(String[] args) {
		System.out.println("Azure Storage Blob Sample\n ");

		try{
			// Retrieve Storage Connection String
			Properties prop = new Properties();
			prop.load(App.class.getClassLoader().getResourceAsStream("config.properties"));
			String storageConnectionString = prop.getProperty("StorageConnectionString");
			
			// Block blob basics
			System.out.println("Block Blob Sample");
			BasicStorageBlockBlobOperations(storageConnectionString); 
		}
		catch (IOException ex){
			System.out.print("IOException encountered: ");
			System.out.println(ex.getMessage());
			System.exit(-1);
		}
	}
	
	// Basic operations to work with block blobs
	public static void BasicStorageBlockBlobOperations(String storageConnectionString){
		try {
			String imageToUpload = "HelloWorld.png";

			// Retrieve storage account information from connection string
			// How to create a storage connection string - https://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-blob-storage/#set-up-an-azure-storage-connection-string
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

			// Create a blob client for interacting with the blob service.
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

			// Create a container for organizing blobs within the storage account.
			System.out.println("1. Creating Container");
			CloudBlobContainer container = blobClient.getContainerReference("democontainerblockblob" + UUID.randomUUID().toString().replace("-", ""));
			container.createIfNotExists();

			// To view the uploaded blob in a browser, you have two options. The first option is to use a Shared Access Signature (SAS) token to delegate 
			// access to the resource. See the documentation links at the top for more information on SAS. The second approach is to set permissions 
			// to allow public access to blobs in this container. Uncomment the three lines of code below to use this approach. Then you can view the image 
			// using: https://[InsertYourStorageAccountNameHere].blob.core.windows.net/democontainerblockblob[randomUUID]/HelloWorld.png

			// BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
			// containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
			// container.uploadPermissions(containerPermissions);

			// Upload a BlockBlob to the newly created container
			System.out.println("2. Uploading BlockBlob");
			CloudBlockBlob blockBlob = container.getBlockBlobReference(imageToUpload);
			File sourceFile = new File(imageToUpload);
			blockBlob.upload(new FileInputStream(sourceFile), sourceFile.length());

			// List all the blobs in the container 
			System.out.println("3. List Blobs in Container");
			for (ListBlobItem blob : container.listBlobs()) {
				// Output URI of each blob
				System.out.println("- " + blob.getUri());
			}

			// Download a blob to your file system
			System.out.println("4. Download Blob from " + blockBlob.getUri());
			File destinationFile = new File(sourceFile.getParentFile(), "CopyOf" + imageToUpload);
			blockBlob.downloadToFile(destinationFile.getAbsolutePath());
			
			// Clean up after the demo 
			System.out.println("5. Delete block Blob");
			blockBlob.delete();
			
			System.out.println("6. Delete Container");
			container.delete();
		}
		catch (FileNotFoundException fileNotFoundException) {
			System.out.print("FileNotFoundException encountered: ");
			System.out.println(fileNotFoundException.getMessage());
			System.exit(-1);
		}
		catch (StorageException storageException) {
			System.out.print("StorageException encountered: ");
			System.out.println(storageException.getMessage());
			System.exit(-1);
		}
		catch (Exception e) {
			System.out.print("Exception encountered: ");
			System.out.println(e.getMessage());
			System.exit(-1);
		}
	}
}