//----------------------------------------------------------------------------------
// Microsoft Developer & Platform Evangelism
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
//----------------------------------------------------------------------------------
// The example companies, organizations, products, domain names,
// e-mail addresses, logos, people, places, and events depicted
// herein are fictitious.  No association with any real company,
// organization, product, domain name, email address, logo, person,
// places, or events is intended or should be inferred.
//----------------------------------------------------------------------------------

package azure_storage.storage_blob_java_getting_started;

import java.io.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

/// <summary>
/// Azure Storage Blob Sample - Demonstrate how to use the Blob Storage service. 
/// Blob storage stores unstructured data such as text, binary data, documents or media files. 
/// Blobs can be accessed from anywhere in the world via HTTP or HTTPS.
/// 
/// Documentation References: 
/// - What is a Storage Account - http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/
/// - Getting Started with Blobs - http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-blob-storage/
/// - Blob Service Concepts - http://msdn.microsoft.com/en-us/library/dd179376.aspx 
/// - Blob Service REST API - http://msdn.microsoft.com/en-us/library/dd135733.aspx
/// - Blob Service Java API - http://azure.github.io/azure-storage-java/
/// - Delegating Access with Shared Access Signatures - http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-shared-access-signature-part-1/
/// - Storage Emulator - http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx
/// </summary>

public class App {
	// *************************************************************************************************************************
	// Instructions: This sample can be run by updating storageConnectionString with your Storage account name and account key
	//
	//      1. Create a Storage Account through the Azure Portal and replace AccountName and AccountKey in 
	//         storageConnectionString with your account name and account key. See http://go.microsoft.com/fwlink/?LinkId=325277 for more information
	//      2. Set breakpoints and run the project. 
	// 
	// *************************************************************************************************************************
	public static final String storageConnectionString =
			"DefaultEndpointsProtocol=https;"
					+ "AccountName=AccountName;"
					+ "AccountKey=AccountKey";

	public static void main(String[] args) {
		System.out.println("Azure Storage Blob Sample\n ");

		// Block blob basics
		System.out.println("Block Blob Sample");
		BasicStorageBlockBlobOperations();

		System.out.println("Press enter to exit");
		try
		{
			System.in.read();
		}  
		catch(Exception e)
		{}  
	}

	/// <summary>
    /// Basic operations to work with block blobs
    /// </summary>
	public static void BasicStorageBlockBlobOperations(){
		try {
			String ImageToUpload = "HelloWorld.png";

			// Retrieve storage account information from connection string
			// How to create a storage connection string - https://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-blob-storage/#set-up-an-azure-storage-connection-string
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

			// Create a blob client for interacting with the blob service.
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

			// Create a container for organizing blobs within the storage account.
			System.out.println("1. Creating Container");
			CloudBlobContainer container = blobClient.getContainerReference("democontainerblockblob");
			container.createIfNotExists();

			// To view the uploaded blob in a browser, you have two options. The first option is to use a Shared Access Signature (SAS) token to delegate 
			// access to the resource. See the documentation links at the top for more information on SAS. The second approach is to set permissions 
			// to allow public access to blobs in this container. Uncomment the three lines of code below to use this approach. Then you can view the image 
			// using: https://[InsertYourStorageAccountNameHere].blob.core.windows.net/democontainerblockblob/HelloWorld.png

			// BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
			// containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
			// container.uploadPermissions(containerPermissions);

			// Upload a BlockBlob to the newly created container
			System.out.println("2. Uploading BlockBlob");
			CloudBlockBlob blockBlob = container.getBlockBlobReference(ImageToUpload);
			File sourceFile = new File(ImageToUpload);
			blockBlob.upload(new FileInputStream(sourceFile), sourceFile.length());

			// List all the blobs in the container 
			System.out.println("3. List Blobs in Container");
			for (ListBlobItem blob : container.listBlobs()) {
				// Output URI of each blob
				System.out.println("- " + blob.getUri());
			}

			// Download a blob to your file system
			System.out.println("4. Download Blob from " + blockBlob.getUri());
			File destinationFile = new File(sourceFile.getParentFile(), "CopyOf" + ImageToUpload);
			blockBlob.downloadToFile(destinationFile.getAbsolutePath());

			// Clean up after the demo 
			System.out.println("5. Delete block Blob");
			blockBlob.delete();

			// When you delete a container it could take several seconds before you can recreate a container with the same
			// name - hence to enable you to run the demo in quick succession, the container is not deleted. If you want 
			// to delete the container, uncomment the two lines of code below. 
			
			// System.out.println("6. Delete Container");
			// container.delete();
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