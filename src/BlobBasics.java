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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.PageRange;

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

        System.out.println("Azure Storage Blob sample - Starting.");

        Scanner scan = null;
        CloudBlobClient blobClient = null;
        CloudBlobContainer container1 = null;
        CloudBlobContainer container2 = null;

        try {
            // Create a scanner for user input
            scan = new Scanner(System.in);

            // Create a blob client for interacting with the blob service
            blobClient = getBlobClientReference();

            // Create new containers with randomized names
            System.out.println("\nCreate container for the sample demonstration");
            container1 = createContainer(blobClient, createRandomName("blobbasics-"));
            System.out.println(String.format("\tSuccessfully created the container \"%s\".", container1.getName()));
            container2 = createContainer(blobClient, createRandomName("blobbasics-"));
            System.out.println(String.format("\tSuccessfully created the container \"%s\".", container2.getName()));

            // Demonstrate block blobs
            System.out.println("\nBasic block blob operations\n");
            try {
                basicBlockBlobOperations(container1);
            }
            catch (StorageException s) {
                if (s.getErrorCode().equals("BlobTypeNotSupported")) {
                    System.out.println(String.format("\t\tError: %s", s.getMessage()));
                }
                else {
                    throw s;
                }
            }

            // Demonstrate append blobs
            System.out.println("\nBasic append blob operations\n");
            try {
                basicAppendBlobOperations(container1);
            }
            catch (StorageException s) {
                if (s.getErrorCode().equals("BlobTypeNotSupported")) {
                    System.out.println(String.format("\t\tError: %s", s.getMessage()));
                }
                else if (s.getErrorCode().equals("FeatureNotSupportedByEmulator")) {
                    System.out.println("\t\tError: The append blob feature is currently not supported by the Storage Emulator.");
                    System.out.println("\t\tPlease run the sample against your Azure Storage account by updating the config.properties file.");
                }
                else {
                    throw s;
                }
            }

            // Demonstrate page blobs
            System.out.println("\nBasic page blob operations\n");
            try {
                basicPageBlobOperations(container2);
            }
            catch (StorageException s) {
                if (s.getErrorCode().equals("BlobTypeNotSupported")) {
                    System.out.println(String.format("\t\tError: %s", s.getMessage()));
                }
                else {
                    throw s;
                }
            }

            // Enumerate all containers starting with the prefix "blobbasics-" and list all blobs
            System.out.println("\nEnumerate all containers and starting with the prefix \"blobbasics-\" list all blobs");
            for (CloudBlobContainer container : blobClient.listContainers("blobbasics-")) {
                System.out.println(String.format("\tContainer: %s", container.getName()));
                for (ListBlobItem blob : container.listBlobs()) {
                    if (blob instanceof CloudBlob) {
                        System.out.println(String.format("\t\t%s\t: %s", ((CloudBlob) blob).getProperties().getBlobType(), blob.getUri().toString()));
                    }
                }
            }

            // Acquire a lease on a container so that another client cannot write to it or delete it
            System.out.println("\nAcquiring a lease on a container to prevent writes and deletes.");
            container1.acquireLease();
            System.out.println(String.format("\tSuccessfully acquired a lease on container %s. Lease state: %s.", container1.getName(), container1.getProperties().getLeaseStatus().toString()));
            container1.breakLease(0);
            System.out.println(String.format("\tSuccessfully broke the lease on container %s. Lease state: %s.", container1.getName(), container1.getProperties().getLeaseStatus().toString()));

            // To view the uploaded blobs in a browser, you have two options.
            //   - The first option is to use a Shared Access Signature (SAS) token to delegate access to the resource.
            //     See the documentation links at the top for more information on SAS.
            //   - The second approach is to set permissions to allow public access to blobs in this container.
            //     Uncomment the the lines of code below to use this approach.
            BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
            containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
            //container1.uploadPermissions(containerPermissions);
            //container2.uploadPermissions(containerPermissions);

        }
        catch (Throwable t) {
            printException(t);
        }
        finally {
            // Delete the containers (If you do not want to delete the container comment out the block of code below)
            System.out.print("\nDelete the containers. Press any key to continue...");
            scan.nextLine();

            if (container1 != null && container1.deleteIfExists() == true) {
                System.out.println(String.format("\tSuccessfully deleted the container: %s", container1.getName()));
            }

            if (container2 != null && container2.deleteIfExists() == true) {
                System.out.println(String.format("\tSuccessfully deleted the container: %s", container2.getName()));
            }

            // Close the scanner
            if (scan != null) {
                scan.close();
            }
        }

        System.out.println("\nAzure Storage Blob sample - Completed.\n");
    }

    /**
     * Validates the connection string and returns the storage blob client.
     * The connection string must be in the Azure connection string format.
     *
     * @return The newly created CloudBlobClient object
     *
     * @throws RuntimeException
     * @throws IOException
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     */
    private static CloudBlobClient getBlobClientReference() throws RuntimeException, IOException, IllegalArgumentException, URISyntaxException, InvalidKeyException {

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

        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(prop.getProperty("StorageConnectionString"));
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

        return storageAccount.createCloudBlobClient();
    }

    /**
     * Creates and returns a container for the sample application to use.
     *
     * @param blobClient CloudBlobClient object
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
    private static CloudBlobContainer createContainer(CloudBlobClient blobClient, String containerName) throws StorageException, RuntimeException, IOException, InvalidKeyException, IllegalArgumentException, URISyntaxException, IllegalStateException {

        // Create a new container
        CloudBlobContainer container = blobClient.getContainerReference(containerName);
        try {
            if (container.createIfNotExists() == false) {
                throw new IllegalStateException(String.format("Container with name \"%s\" already exists.", containerName));
            }
        }
        catch (StorageException s) {
            if (s.getCause() instanceof java.net.ConnectException) {
                System.out.println("Caught connection exception from the client. If running with the default configuration please make sure you have started the storage emulator.");
            }
            throw s;
        }

        return container;
    }

    /**
     * Demonstrates the basic operations with a block blob.
     *
     * @param container The CloudBlobContainer object to work with
     *
     * @throws Throwable
     * @throws StorageException
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    private static void basicBlockBlobOperations(CloudBlobContainer container) throws Throwable, StorageException, IOException, IllegalArgumentException, URISyntaxException {

        // Create sample files for use
        Random random = new Random();

        System.out.println("\tCreating sample files between 128KB-256KB in size for upload demonstration.");
        File tempFile1 = createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile1.getAbsolutePath()));
        File tempFile2 = createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile2.getAbsolutePath()));
        File tempFile3 = createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile3.getAbsolutePath()));

        // Upload a sample file as a block blob
        System.out.println("\n\tUpload a sample file as a block blob.");
        CloudBlockBlob blockBlob1 = container.getBlockBlobReference("blockblob1.tmp");
        blockBlob1.uploadFromFile(tempFile1.getAbsolutePath());
        System.out.println("\t\tSuccessfully uploaded the blob.");

        // Create a read-only snapshot of the blob
        System.out.println("\n\tCreate a read-only snapshot of the blob.");
        CloudBlob blockBlob1Snapshot = blockBlob1.createSnapshot();
        System.out.println("\t\tSuccessfully created a snapshot of the blob.");

        // Modify the blob by overwriting it
        System.out.println("\n\tOverwrite the blob by uploading the second sample file.");
        blockBlob1.uploadFromFile(tempFile2.getAbsolutePath());
        System.out.println("\t\tSuccessfully overwrote the blob.");

        // Acquire a lease on the blob so that another client cannot write to it or delete it
        System.out.println("\n\tAcquiring a lease on the blog to prevent writes and deletes.");
        blockBlob1.acquireLease();
        System.out.println(String.format("\t\tSuccessfully acquired a lease on blob %s. Lease state: %s.", blockBlob1.getName(), blockBlob1.getProperties().getLeaseStatus().toString()));
        blockBlob1.breakLease(0);
        System.out.println(String.format("\t\tSuccessfully broke the lease on blob %s. Lease state: %s.", blockBlob1.getName(), blockBlob1.getProperties().getLeaseStatus().toString()));

        // Upload a sample file as a block blob using a block list
        System.out.println("\n\tUpload the third sample file as a block blob using a block list.");
        CloudBlockBlob blockBlob2 = container.getBlockBlobReference("blockblob2.tmp");
        uploadFileBlocksAsBlockBlob(blockBlob2, tempFile3.getAbsolutePath());
        System.out.println("\t\tSuccessfully uploaded the blob using a block list.");

        // Download the block list for the block blob
        System.out.println("\n\tDownload the block list.");
        for (BlockEntry blockEntry : blockBlob2.downloadBlockList()) {
            System.out.println(String.format("\t\tBlock id: %s (%s), size: %s", new String(Base64.getDecoder().decode(blockEntry.getId())), blockEntry.getId(), blockEntry.getSize()));
        }

        // Create sample file for copy demonstration
        System.out.println("\n\tCreating sample file between 10MB-15MB in size for abort copy demonstration.");
        File tempFile4 = createTempLocalFile("blockblob-", ".tmp", (10 * 1024 * 1024) + random.nextInt(5 * 1024 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile4.getAbsolutePath()));

        // Upload a sample file as a block blob
        System.out.println("\n\tUpload the sample file as a block blob.");
        CloudBlockBlob blockBlob3 = container.getBlockBlobReference("blockblob3.tmp");
        blockBlob3.uploadFromFile(tempFile4.getAbsolutePath());
        System.out.println("\t\tSuccessfully uploaded the blob.");

        // Copy the blob
        System.out.println(String.format("\n\tCopying blob \"%s\".", blockBlob3.getUri().toURL()));
        CloudBlockBlob blockBlob3Copy = container.getBlockBlobReference(blockBlob3.getName() + ".copy");
        blockBlob3Copy.startCopy(blockBlob3);
        waitForCopyToComplete(blockBlob3Copy);
        System.out.println("\t\tSuccessfully copied the blob.");

        // Abort copying the blob
        System.out.println(String.format("\n\tAborting while copying blob \"%s\".", blockBlob3.getUri().toURL()));
        CloudBlockBlob blockBlob3CopyAborted = container.getBlockBlobReference(blockBlob3.getName() + ".copyaborted");
        boolean copyAborted = true;
        String copyId = blockBlob3CopyAborted.startCopy(blockBlob3);
        try {
            blockBlob3CopyAborted.abortCopy(copyId);
        }
        catch (StorageException ex) {
            if (ex.getErrorCode().equals("NoPendingCopyOperation")) {
                copyAborted = false;
            } else {
                throw ex;
            }
        }
        if (copyAborted == true) {
            System.out.println("\t\tSuccessfully aborted copying the blob.");
        } else {
            System.out.println("\t\tFailed to abort copying the blob because the copy finished before we could abort.");
        }

        // Download the blobs and its snapshot
        System.out.println("\n\tDownload the blobs and its snapshots.");

        String downloadedBlobPath = String.format("%ssnapshotof-%s", System.getProperty("java.io.tmpdir"), blockBlob1Snapshot.getName());
        System.out.println(String.format("\t\tDownload the blob snapshot from \"%s\" to \"%s\".", blockBlob1Snapshot.getUri().toURL(), downloadedBlobPath));
        blockBlob1Snapshot.downloadToFile(downloadedBlobPath);
        new File(downloadedBlobPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob snapshot.");

        downloadedBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), blockBlob1.getName());
        System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", blockBlob1.getUri().toURL(), downloadedBlobPath));
        blockBlob1.downloadToFile(downloadedBlobPath);
        new File(downloadedBlobPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob.");

        downloadedBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), blockBlob2.getName());
        System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", blockBlob2.getUri().toURL(), downloadedBlobPath));
        blockBlob2.downloadToFile(downloadedBlobPath);
        new File(downloadedBlobPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob.");

        // Delete a blob and its snapshots
        System.out.println(String.format("\n\tDelete the blob \"%s\" its snapshots.", blockBlob1.getName()));
        blockBlob1.delete(DeleteSnapshotsOption.INCLUDE_SNAPSHOTS, null, null, null);
        System.out.println("\t\tSuccessfully deleted the blob and its snapshots.");
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
        System.out.println("\t\t\tSuccessfully uploaded the blob sparsely.");

        // Create a read-only snapshot of the blob
        System.out.println("\n\tCreate a read-only snapshot of the blob.");
        CloudBlob pageBlobSnapshot = pageBlob.createSnapshot();
        System.out.println("\t\tSuccessfully created a snapshot of the blob.");

        // Upload new pages to the blob, modify and clear existing pages
        System.out.println("\n\tModify the blob by uploading new pages to the blob and clearing existing pages.");
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
        System.out.println("\t\t\tSuccessfully modified the blob.");

        // Query valid page ranges
        System.out.println("\n\tQuery valid page ranges.");
        for (PageRange pageRange : pageBlob.downloadPageRanges()) {
            System.out.println(String.format("\t\tRange start offset: %d, end offset: %d", pageRange.getStartOffset(), pageRange.getEndOffset()));
        }

        // Query page range diff between snapshots
        System.out.println("\n\tQuery page range diff between the snapshot and the current state.");
        for (PageRange pageRange : pageBlob.downloadPageRangesDiff(pageBlobSnapshot.getSnapshotID())) {
            System.out.println(String.format("\t\tRange start offset: %d, end offset: %d", pageRange.getStartOffset(), pageRange.getEndOffset()));
        }

        // Download the blob and its snapshot
        System.out.println("\n\tDownload the blob and its snapshot.");

        String downloadedPageBlobSnapshotPath = String.format("%ssnapshotof-%s", System.getProperty("java.io.tmpdir"), pageBlobSnapshot.getName());
        System.out.println(String.format("\t\tDownload the blob snapshot from \"%s\" to \"%s\".", pageBlobSnapshot.getUri().toURL(), downloadedPageBlobSnapshotPath));
        pageBlobSnapshot.downloadToFile(downloadedPageBlobSnapshotPath);
        new File(downloadedPageBlobSnapshotPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob snapshot.");

        String downloadedPageBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), pageBlob.getName());
        System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", pageBlob.getUri().toURL(), downloadedPageBlobPath));
        pageBlob.downloadToFile(downloadedPageBlobPath);
        new File(downloadedPageBlobPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob.");
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
        appendBlob.createOrReplace();
        appendBlob.appendFromFile(tempFile1.getAbsolutePath());
        appendBlob.appendFromFile(tempFile2.getAbsolutePath());
        System.out.println("\t\tSuccessfully created the append blob and appended data to it.");

        // Write random data blocks to the end of the append blob
        byte[] randomBytes = new byte[4096];
        for (int i = 0; i < 8; i++) {
            random.nextBytes(randomBytes);
            appendBlob.appendFromByteArray(randomBytes, 0, 4096);
        }

        // Download the blob
        if (appendBlob != null) {
            System.out.println("\n\tDownload the blob.");
            String downloadedAppendBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), appendBlob.getName());
            System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", appendBlob.getUri().toURL(), downloadedAppendBlobPath));
            appendBlob.downloadToFile(downloadedAppendBlobPath);
            new File(downloadedAppendBlobPath).deleteOnExit();
            System.out.println("\t\t\tSuccessfully downloaded the blob.");
        }
    }

    /**
     * Creates and returns a temporary local file for use by the sample.
     *
     * @param blockBlob CloudBlockBlob object.
     * @param filePath The path to the file to be uploaded.
     *
     * @throws Throwable
     */
    private static void uploadFileBlocksAsBlockBlob(CloudBlockBlob blockBlob, String filePath) throws Throwable {

        FileInputStream fileInputStream = null;
        try {
            // Open the file
            fileInputStream = new FileInputStream(filePath);

            // Split the file into 32K blocks (block size deliberately kept small for the demo) and upload all the blocks
            int blockNum = 0;
            String blockId = null;
            String blockIdEncoded = null;
            ArrayList<BlockEntry> blockList = new ArrayList<BlockEntry>();
            while (fileInputStream.available() > (32 * 1024)) {
                blockId = String.format("%05d", blockNum);
                blockIdEncoded = Base64.getEncoder().encodeToString(blockId.getBytes());
                blockBlob.uploadBlock(blockIdEncoded, fileInputStream, (32 * 1024));
                blockList.add(new BlockEntry(blockIdEncoded));
                blockNum++;
            }
            blockId = String.format("%05d", blockNum);
            blockIdEncoded = Base64.getEncoder().encodeToString(blockId.getBytes());
            blockBlob.uploadBlock(blockIdEncoded, fileInputStream, fileInputStream.available());
            blockList.add(new BlockEntry(blockIdEncoded));

            // Commit the blocks
            blockBlob.commitBlockList(blockList);
        }
        catch (Throwable t) {
            throw t;
        }
        finally {
            // Close the file output stream writer
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }
    }

    /**
     * Wait until the copy complete.
     *
     * @param blob Target of the copy operation
     *
     * @throws InterruptedException
     * @throws StorageException
     */
    private static void waitForCopyToComplete(CloudBlob blob) throws InterruptedException, StorageException {
        CopyStatus copyStatus = CopyStatus.PENDING;
        while (copyStatus == CopyStatus.PENDING) {
            Thread.sleep(1000);
            copyStatus = blob.getCopyState().getStatus();
        }
    }

    /**
     * Creates and returns a randomized name based on the prefix file for use by the sample.
     *
     * @param namePrefix The prefix string to be used in generating the name.
     * @return The randomized name
     */
    private static String createRandomName(String namePrefix) {

        return namePrefix + UUID.randomUUID().toString().replace("-", "");
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
    public static void printException(Throwable t) {

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        if (t instanceof StorageException) {
            if (((StorageException) t).getExtendedErrorInformation() != null) {
                System.out.println(String.format("\nError: %s", ((StorageException) t).getExtendedErrorInformation().getErrorMessage()));
            }
        }
        System.out.println(String.format("Exception details:\n%s", stringWriter.toString()));
    }
}