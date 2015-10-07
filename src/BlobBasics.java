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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
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

    /**
     * Azure Storage Blob Sample
     *
     * @param args No input arguments are expected from users.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        System.out.println("Azure Storage Blob sample - Starting.\n");

        Scanner scan = null;
        CloudBlobContainer container = null;
        try {
            // Create a scanner for user input
            scan = new Scanner(System.in);

            // Create new container with a randomized name
            String containerName = "blobbasics" + UUID.randomUUID().toString().replace("-", "");
            System.out.println(String.format("\n1. Create a container with name \"%s\"", containerName));
            try {
                container = createContainer(containerName);
            }
            catch (IllegalStateException e) {
                System.out.println(String.format("\tContainer already exists."));
                throw e;
            }
            System.out.println("\tSuccessfully created the container.");

            // Demonstrate block blobs
            System.out.println("\n2. Basic block blob operations");
            basicBlockBlobOperations(container);

            // Demonstrate page blobs
            System.out.println("\n3. Basic page blob operations");
            basicPageBlobOperations(container);

            // Demonstrate append blobs
            System.out.println("\n4. Basic append blob operations");
            basicAppendBlobOperations(container);

            // List all the blobs in the container
            System.out.println("\n5. List all blobs in the container");
            for (ListBlobItem blob : container.listBlobs()) {
                if (blob instanceof CloudBlob) {
                    System.out.println(String.format("\t%s\t: %s", ((CloudBlob) blob).getProperties().getBlobType(), blob.getUri().toString()));
                }
            }

            // To view the uploaded blobs in a browser, you have two options.
            //   - The first option is to use a Shared Access Signature (SAS) token to delegate access to the resource.
            //     See the documentation links at the top for more information on SAS.
            //   - The second approach is to set permissions to allow public access to blobs in this container.
            //     Uncomment the the line of code below to use this approach.
            BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
            containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
            //container.uploadPermissions(containerPermissions);

            // Delete the blobs and any of its snapshots
            System.out.print("\n6. Delete the blobs and any of its snapshots. Press any key to continue...");
            scan.nextLine();
            for (ListBlobItem blob : container.listBlobs()) {
                if (blob instanceof CloudBlob) {
                    ((CloudBlob) blob).delete(DeleteSnapshotsOption.INCLUDE_SNAPSHOTS, null, null, null);
                }
            }
            System.out.println("\tSuccessfully deleted the blobs and its snapshots.");

        }
        catch (Throwable t) {
            printException(t);
        }
        finally {
            // Delete the container (If you do not want to delete the container comment out the block of code below)
            if (container != null)
            {
                System.out.print("\n7. Delete the container. Press any key to continue...");
                scan.nextLine();
                if (container.deleteIfExists() == true) {
                    System.out.println("\tSuccessfully deleted the container.");
                }
                else {
                    System.out.println("\tNothing to delete.");
                }
            }

            // Close the scanner
            if (scan != null) {
                scan.close();
            }
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
     * Demonstrates the basic operations with a block blob.
     *
     * @param container The CloudBlobContainer object to work with
     *
     * @throws StorageException
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    private static void basicBlockBlobOperations(CloudBlobContainer container) throws StorageException, IOException, IllegalArgumentException, URISyntaxException {

        // Create sample files for use
        Random random = new Random();
        System.out.println("\tCreating sample files between 128KB-256KB in size for upload demonstration.");
        File tempFile1 = createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile1.getAbsolutePath()));
        File tempFile2 = createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile2.getAbsolutePath()));

        // Upload a sample file as a block blob
        System.out.println("\n\tUpload a sample file as a block blob.");
        CloudBlockBlob blockBlob = container.getBlockBlobReference("blockblob.tmp");
        blockBlob.uploadFromFile(tempFile1.getAbsolutePath());
        System.out.println("\t\tSuccessfully uploaded the blob.");

        // Create a read-only snapshot of the blob
        System.out.println("\n\tCreate a read-only snapshot of the blob.");
        CloudBlob blockBlobSnapshot = blockBlob.createSnapshot();
        System.out.println("\t\tSuccessfully created a snapshot of the blob.");

        // Modify the blob by overwriting it
        System.out.println("\n\tOverwrite the blob by uploading the second sample file.");
        blockBlob.uploadFromFile(tempFile2.getAbsolutePath());
        System.out.println("\t\tSuccessfully overwrote the blob.");

        // Download the blob and its snapshot
        System.out.println("\n\tDownload the blob and its snapshot.");

        String downloadedBlockBlobSnapshotPath = String.format("%ssnapshotof-%s", System.getProperty("java.io.tmpdir"), blockBlobSnapshot.getName());
        System.out.println(String.format("\t\tDownload the blob snapshot from \"%s\" to \"%s\".", blockBlobSnapshot.getUri().toURL(), downloadedBlockBlobSnapshotPath));
        blockBlobSnapshot.downloadToFile(downloadedBlockBlobSnapshotPath);
        new File(downloadedBlockBlobSnapshotPath).deleteOnExit();
        System.out.println("\t\tSuccessfully downloaded the blob snapshot.");

        String downloadedBlockBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), blockBlob.getName());
        System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", blockBlob.getUri().toURL(), downloadedBlockBlobPath));
        blockBlob.downloadToFile(downloadedBlockBlobPath);
        new File(downloadedBlockBlobPath).deleteOnExit();
        System.out.println("\t\tSuccessfully downloaded the blob.");
    }

    /**
     * Demonstrates the basic operations with a page blob.
     *
     * @param container The CloudBlobContainer object to work with
     *
     * @throws StorageException
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    private static void basicPageBlobOperations(CloudBlobContainer container) throws StorageException, IOException, IllegalArgumentException, URISyntaxException {

        // Create sample files for use. We use a file whose size is aligned to 512 bytes since page blobs are expected to be aligned to 512 byte pages.
        System.out.println("\tCreating sample file 128KB in size (aligned to 512 bytes) for upload demonstration.");
        File tempFile = createTempLocalFile("pageblob-", ".tmp", (128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile.getAbsolutePath()));

        // Upload the sample file sparsely as a page blob (Only upload certain ranges of the file)
        System.out.println("\n\tUpload the sample file sparsely as a page blob.");
        System.out.println("\t\tCreating an empty page blob of the same size as the sample file.");
        CloudPageBlob pageBlob = container.getPageBlobReference("pageblob.tmp");
        pageBlob.create(tempFile.length()); // This will throw an IllegalArgumentException if the size if not aligned to 512 bytes.

        // Upload selective pages to the blob
        System.out.println("\t\tUploading selective pages to the blob.");
        FileInputStream tempFileInputStream = null;
        try {
            tempFileInputStream = new FileInputStream(tempFile);
            System.out.println("\t\t\tUploading range start: 0, length: 1024.");
            pageBlob.uploadPages(tempFileInputStream, 0, 1024);
            System.out.println("\t\t\tUploading range start: 4096, length: 1536.");
            pageBlob.uploadPages(tempFileInputStream, 4096, 1536);
        }
        catch (Throwable t) {
            throw t;
        }
        finally {
            if (tempFileInputStream != null) {
                tempFileInputStream.close();
            }
        }
        System.out.println("\t\tSuccessfully uploaded the blob sparsely.");

        // Create a read-only snapshot of the blob
        System.out.println("\n\tCreate a read-only snapshot of the blob.");
        CloudBlob pageBlobSnapshot = pageBlob.createSnapshot();
        System.out.println("\t\tSuccessfully created a snapshot of the blob.");

        // Upload new pages to the blob, modify and clear existing pages
        System.out.println("\t\tModify the blob by uploading new pages to the blob and clearing existing pages.");
        tempFileInputStream = null;
        try {
            tempFileInputStream = new FileInputStream(tempFile);
            System.out.println("\t\t\tUploading range start: 8192, length: 4096.");
            tempFileInputStream.getChannel().position(8192);
            pageBlob.uploadPages(tempFileInputStream, 8192, 4096);
            System.out.println("\t\t\tClearing range start: 4608, length: 512.");
            pageBlob.clearPages(4608, 512);
        }
        catch (Throwable t) {
            throw t;
        }
        finally {
            if (tempFileInputStream != null) {
                tempFileInputStream.close();
            }
        }
        System.out.println("\t\tSuccessfully modified the blob.");

        // Download the blob and its snapshot
        System.out.println("\n\tDownload the blob and its snapshot.");

        String downloadedPageBlobSnapshotPath = String.format("%ssnapshotof-%s", System.getProperty("java.io.tmpdir"), pageBlobSnapshot.getName());
        System.out.println(String.format("\t\tDownload the blob snapshot from \"%s\" to \"%s\".", pageBlobSnapshot.getUri().toURL(), downloadedPageBlobSnapshotPath));
        pageBlobSnapshot.downloadToFile(downloadedPageBlobSnapshotPath);
        new File(downloadedPageBlobSnapshotPath).deleteOnExit();
        System.out.println("\t\tSuccessfully downloaded the blob snapshot.");

        String downloadedPageBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), pageBlob.getName());
        System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", pageBlob.getUri().toURL(), downloadedPageBlobPath));
        pageBlob.downloadToFile(downloadedPageBlobPath);
        new File(downloadedPageBlobPath).deleteOnExit();
        System.out.println("\t\tSuccessfully downloaded the blob.");
    }

    /**
     * Demonstrates the basic operations with a append blob.
     *
     * @param container The CloudBlobContainer object to work with
     *
     * @throws StorageException
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    private static void basicAppendBlobOperations(CloudBlobContainer container) throws StorageException, IOException, IllegalArgumentException, URISyntaxException {

        // Create sample files for use
        Random random = new Random();
        System.out.println("\tCreating sample files between 128KB-256KB in size for upload demonstration.");
        File tempFile1 = createTempLocalFile("appendblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile1.getAbsolutePath()));
        File tempFile2 = createTempLocalFile("appendblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile2.getAbsolutePath()));

        // Create an append blob and append data to it from the sample file
        System.out.println("\n\tCreate an empty append blob and append data to it from the sample files.");
        CloudAppendBlob appendBlob = container.getAppendBlobReference("appendblob.tmp");
        try {
            appendBlob.createOrReplace();
            appendBlob.appendFromFile(tempFile1.getAbsolutePath());
            appendBlob.appendFromFile(tempFile2.getAbsolutePath());
            System.out.println("\t\tSuccessfully created the append blob and appended data to it.");
        }
        catch (StorageException s) {
            if (s.getErrorCode().equals("FeatureNotSupportedByEmulator")) {
                appendBlob = null;
                System.out.println("\t\tThe append blob feature is currently not supported by the Storage Emulator.");
                System.out.println("\t\tPlease run the sample against your Azure Storage account by updating the config.properties file.");
            }
            else {
                throw s;
            }
        }

        // Download the blob
        if (appendBlob != null) {
            System.out.println("\n\tDownload the blob.");
            String downloadedAppendBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), appendBlob.getName());
            System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", appendBlob.getUri().toURL(), downloadedAppendBlobPath));
            appendBlob.downloadToFile(downloadedAppendBlobPath);
            new File(downloadedAppendBlobPath).deleteOnExit();
            System.out.println("\t\tSuccessfully downloaded the blob.");
        }
    }

    /**
     * Creates and returns a temporary local file for use by the sample.
     *
     * @param tempFileNamePrefix The prefix string to be used in generating the file's name.
     * @param tempFileNameSuffix The suffix string to be used in generating the file's name.
     * @param bytesToWrite The number of bytes to write to file.
     * @return The newly created File object
     *
     * @throws IOException
     * @throws IllegalArgumentException
     */
    private static File createTempLocalFile(String tempFileNamePrefix, String tempFileNameSuffix, int bytesToWrite) throws IOException, IllegalArgumentException{

        File tempFile = null;
        FileOutputStream tempFileOutputStream = null;
        try {
            // Create the temporary file
            tempFile = File.createTempFile(tempFileNamePrefix, tempFileNameSuffix);

            // Write random bytes to the file if requested
            Random random = new Random();
            byte[] randomBytes = new byte[4096];
            tempFileOutputStream = new FileOutputStream(tempFile);
            while (bytesToWrite > 0) {
                random.nextBytes(randomBytes);
                tempFileOutputStream.write(randomBytes, 0, (bytesToWrite > 4096) ? 4096 : bytesToWrite);
                bytesToWrite -= 4096;
            }
        }
        catch (Throwable t) {
            throw t;
        }
        finally {
            // Close the file output stream writer
            if (tempFileOutputStream != null) {
                tempFileOutputStream.close();
            }

            // Set the temporary file to delete on exit
            if (tempFile != null) {
                tempFile.deleteOnExit();
            }
        }

        return tempFile;
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