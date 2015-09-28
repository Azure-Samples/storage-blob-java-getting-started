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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

/*
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
 *  - Storage Emulator - http://azure.microsoft.com/en-us/documentation/articles/storage-use-emulator/
 *
 * Instructions:
 *      This sample can be run using either the Azure Storage Emulator or your Azure Storage
 *      account by updating the config.properties file with your "AccountName" and "Key".
 *
 *      To run the sample using the Storage Emulator (default option - Only available on Microsoft Windows OS)
 *          1.  Start the Azure Storage Emulator by pressing the Start button or the Windows key and searching for it
 *              by typing "Azure Storage Emulator". Select it from the list of applications to start it.
 *          2.  Set breakpoints and run the project.
 *
 *      To run the sample using the Storage Service
 *          1.  Open the config.properties file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and
 *              uncomment the connection string for the storage service (AccountName=[]...)
 *          2.  Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in the config.properties file.
 *              See https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/ for more information.
 *          3.  Set breakpoints and run the project.
 */
public class BlobBasics {

    protected static CloudBlobContainer container = null;
    protected final static String containerNamePrefix = "blobbasics";
    protected final static String tempFileNamePrefix = "HelloWorld-";
    protected final static String tempFileNameSuffix = ".txt";

    /**
     * Azure Storage Blob Sample
     *
     * @param args No input arguments are expected from users.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        System.out.println("Azure Storage Blob sample - Starting.\n");

        Scanner scan = null;
        BufferedWriter bufferedWriter = null;
        try {
            // Create a scanner for user input
            scan = new Scanner(System.in);

            // Create a sample file for use
            System.out.println("Creating a sample file for upload demonstration.");
            File tempFile = File.createTempFile(tempFileNamePrefix, tempFileNameSuffix);
            bufferedWriter = new BufferedWriter(new FileWriter(tempFile));
            for (int i = 0; i < 256; i++) {
                bufferedWriter.write("Hello World!!");
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
            System.out.println(String.format("\tSuccessfully created the file \"%s\".", tempFile.getAbsolutePath()));

            // Create new container with a randomized name
            String containerName = containerNamePrefix + UUID.randomUUID().toString().replace("-", "");
            System.out.println(String.format("\n1. Create a container with name \"%s\"", containerName));
            try {
                container = createContainer(containerName);
            }
            catch (IllegalStateException e) {
                System.out.println(String.format("\tContainer already exists."));
                throw e;
            }
            System.out.println("\tSuccessfully created the container.");

            // Upload a local file to the newly created container as a block blob
            System.out.println("\n2. Upload the sample file as a block blob.");
            CloudBlockBlob blockBlob = container.getBlockBlobReference(tempFile.getName());
            File sourceFile = new File(tempFile.getAbsolutePath());
            blockBlob.upload(new FileInputStream(sourceFile), sourceFile.length());
            System.out.println("\tSuccessfully created the container.");

            // List all the blobs in the container
            System.out.println("\n3. List all blobs in the container");
            for (ListBlobItem blob : container.listBlobs()) {
                System.out.println(String.format("\tBLOB\t: %s", blob.getUri().toString()));
            }

            // To view the uploaded blob in a browser, you have two options.
            //   - The first option is to use a Shared Access Signature (SAS) token to delegate access to the resource.
            //     See the documentation links at the top for more information on SAS.
            //   - The second approach is to set permissions to allow public access to blobs in this container.
            //     Uncomment the the line of code below to use this approach.
            BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
            containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
            //container.uploadPermissions(containerPermissions);

            // Download the uploaded blob
            String downloadedBlobPath = String.format("%sCopyOf-%s", System.getProperty("java.io.tmpdir"), tempFile.getName());
            System.out.println(String.format("\n4. Download blob from \"%s\" to \"%s\".", blockBlob.getUri().toURL(), downloadedBlobPath));
            blockBlob.downloadToFile(downloadedBlobPath);
            System.out.println("\tSuccessfully downloaded the blob.");

            // Delete the blob
            System.out.print("\n5. Delete the block blob. Press any key to continue...");
            scan.nextLine();
            blockBlob.delete();
            System.out.println("\tSuccessfully deleted the blob.");
        }
        catch (Throwable t) {
            printException(t);
        }
        finally {
            // Delete the container (If you do not want to delete the container comment out the block of code below)
            if (container != null)
            {
                System.out.print("\n6. Delete the container. Press any key to continue...");
                scan.nextLine();
                if (container.deleteIfExists() == true) {
                    System.out.println("\tSuccessfully deleted the container.");
                }
                else {
                    System.out.println("\tNothing to delete.");
                }
            }

            // Close the scanner
            scan.close();
        }

        System.out.println("\nAzure Storage Blob sample - Completed.\n");
    }

    /**
     * Validates the connection string and returns the storage account.
     * The connection string must be in the Azure connection string format.
     *
     * @param storageConnectionString Connection string for the storage service or the emulator
     * @return The newly created CloudStorageAccount object
     *
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     */
    private static CloudStorageAccount createStorageAccountFromConnectionString(String storageConnectionString) throws IllegalArgumentException, URISyntaxException, InvalidKeyException {

        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
        }
        catch (IllegalArgumentException|URISyntaxException e) {
            System.out.println("\nConnection string specifies an invalid URI.");
            System.out.println("Please confirm the connection string is in the Azure connection string format.");
            throw e;
        }
        catch (InvalidKeyException e) {
            System.out.println("\nConnection string specifies an invalid key.");
            System.out.println("Please confirm the AccountName and AccountKey in the connection string are valid.");
            throw e;
        }

        return storageAccount;
    }

    /**
     * Creates and returns a container for the sample application to use.
     *
     * @param containerName Name of the container to create
     * @return The newly created CloudBlobContainer object
     *
     * @throws StorageException
     * @throws RuntimeException
     * @throws IOException
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     * @throws IllegalStateException
     */
    private static CloudBlobContainer createContainer(String containerName) throws StorageException, RuntimeException, IOException, InvalidKeyException, IllegalArgumentException, URISyntaxException, IllegalStateException {

        // Retrieve the connection string
        Properties prop = new Properties();
        try {
            InputStream propertyStream = BlobBasics.class.getClassLoader().getResourceAsStream("config.properties");
            if (propertyStream != null) {
                prop.load(propertyStream);
            }
            else {
                throw new RuntimeException();
            }
        } catch (RuntimeException|IOException e) {
            System.out.println("\nFailed to load config.properties file.");
            throw e;
        }
        String storageConnectionString = prop.getProperty("StorageConnectionString");

        // Retrieve storage account information from connection string.
        CloudStorageAccount storageAccount = createStorageAccountFromConnectionString(storageConnectionString);

        // Create a blob client for interacting with the blob service
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

        // Create a new container
        CloudBlobContainer container = blobClient.getContainerReference(containerName);
        try {
            if (container.createIfNotExists() == false) {
                throw new IllegalStateException(String.format("Container with name \"%s\" already exists.", containerName));
            }
        }
        catch (StorageException e) {
            System.out.println("\nCaught storage exception from the client.");
            System.out.println("If running with the default configuration please make sure you have started the storage emulator.");
            throw e;
        }

        return container;
    }

    /**
     * Print the exception stack trace
     *
     * @param ex Exception to be printed
     */
    public static void printException(Throwable ex) {

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        ex.printStackTrace(printWriter);
        System.out.println(String.format("Exception details:\n%s\n", stringWriter.toString()));
    }
}