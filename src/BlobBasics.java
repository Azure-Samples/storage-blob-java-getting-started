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

import java.io.*;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.util.*;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.implementation.AzureBlobStorageBuilder;
import com.azure.storage.blob.implementation.AzureBlobStorageImpl;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.options.BlobBeginCopyOptions;
import com.azure.storage.blob.specialized.*;

/**
 * This sample illustrates basic usage of the various Blob Primitives provided
 * in the Storage Client Library including CloudBlobContainer, CloudBlockBlob
 * and CloudBlobClient.
 */
public class BlobBasics {

    /**
     * Azure Storage Blob Sample
     *
     * @throws Exception
     */
    public static void runSamples() throws Exception {

        System.out.println("Azure Storage Blob basic sample - Starting.");

        BlobServiceClient blobServiceClient;
        BlobContainerClient container1 = null;
        BlobContainerClient container2 = null;

        try {
            // Create a blob client for interacting with the blob service
            blobServiceClient = BlobClientProvider.getBlobClientReference();

            // Create new containers with randomized names
            System.out.println("\nCreate container for the sample demonstration");
            container1 = createContainer(blobServiceClient, DataGenerator.createRandomName("blobbasics-"));
            System.out.println(String.format("\tSuccessfully created the container \"%s\".", container1.getBlobContainerName()));
            container2 = createContainer(blobServiceClient, DataGenerator.createRandomName("blobbasics-"));
            System.out.println(String.format("\tSuccessfully created the container \"%s\".", container2.getBlobContainerName()));

            // Demonstrate block blobs
            System.out.println("\nBasic block blob operations\n");
            try {
                basicBlockBlobOperations(container1);
            }
            catch (BlobStorageException s) {
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
            catch (BlobStorageException s) {
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
            catch (BlobStorageException s) {
                if (s.getErrorCode().equals("BlobTypeNotSupported")) {
                    System.out.println(String.format("\t\tError: %s", s.getMessage()));
                }
                else {
                    throw s;
                }
            }

            // Enumerate all containers starting with the prefix "blobbasics-" and list all blobs
            System.out.println("\nEnumerate all containers and starting with the prefix \"blobbasics-\" list all blobs");
            blobServiceClient.listBlobContainers(new ListBlobContainersOptions().setPrefix("blobbasics-"), (Duration)null).forEach(blobContainerItem -> {
                System.out.println(String.format("\tContainer: %s", blobContainerItem.getName()));
                BlobContainerClient containerItem = blobServiceClient.getBlobContainerClient(blobContainerItem.getName());
                containerItem.listBlobs().forEach(blobItem -> {
                    System.out.println(String.format("\t\t%s\t: %s", blobItem.getProperties().getBlobType(), containerItem.getBlobClient(blobItem.getName()).getBlobUrl()));
                });
            });

            // Acquire a lease on a container so that another client cannot write to it or delete it
            System.out.println("\nAcquiring a lease on a container to prevent writes and deletes.");
            BlobLeaseClient blockLeaseBlob = new BlobLeaseClientBuilder().containerClient(container1).buildClient();
            blockLeaseBlob.acquireLease(-1);
            System.out.println(String.format("\tSuccessfully acquired a lease on container %s. Lease state: %s.", container1.getBlobContainerName(), container1.getProperties().getLeaseStatus().toString()));
            blockLeaseBlob.breakLease();
            System.out.println(String.format("\tSuccessfully broke the lease on container %s. Lease state: %s.", container1.getBlobContainerName(), container1.getProperties().getLeaseStatus().toString()));

            // To view the uploaded blobs in a browser, you have two options.
            //   - The first option is to use a Shared Access Signature (SAS) token to delegate access to the resource.
            //     See the documentation links at the top for more information on SAS.
            //   - The second approach is to set permissions to allow public access to blobs in this container.
            //     Uncomment the the lines of code below to use this approach.
            container1.setAccessPolicy(PublicAccessType.CONTAINER,container1.getAccessPolicy().getIdentifiers());
            container2.setAccessPolicy(PublicAccessType.CONTAINER,container2.getAccessPolicy().getIdentifiers());

        }
        catch (Throwable t) {
            PrintHelper.printException(t);
        }
        finally {
            // Delete the containers (If you do not want to delete the container comment out the block of code below)
            System.out.print("\nDelete the containers.");

            if (container1 != null && container1.exists() == true) {
                container1.delete();
                System.out.println(String.format("\tSuccessfully deleted the container: %s", container1.getBlobContainerName()));
            }

            if (container2 != null && container2.exists() == true) {
                container2.delete();
                System.out.println(String.format("\tSuccessfully deleted the container: %s", container2.getBlobContainerName()));
            }
        }

        System.out.println("\nAzure Storage Blob basic sample - Completed.\n");
    }

    /**
     * Creates and returns a container for the sample application to use.
     *
     * @param blobServiceClient CloudBlobClient object
     * @param containerName Name of the container to create
     * @return The newly created CloudBlobContainer object
     *
     * @throws BlobStorageException
     * @throws RuntimeException
     * @throws IOException
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     * @throws IllegalStateException
     */
    private static BlobContainerClient createContainer(BlobServiceClient blobServiceClient, String containerName) throws BlobStorageException, RuntimeException, IOException, InvalidKeyException, IllegalArgumentException, URISyntaxException, IllegalStateException {

        // Create a new container
        if ( blobServiceClient.getBlobContainerClient(containerName).exists() ) {
            throw new IllegalStateException(String.format("Container with name \"%s\" already exists.", containerName));
        }
        try {
            return blobServiceClient.createBlobContainer(containerName);
        } catch (BlobStorageException s) {
            if (s.getCause() instanceof java.net.ConnectException) {
                System.out.println("Caught connection exception from the client. If running with the default configuration please make sure you have started the storage emulator.");
            }
            throw s;
        }
    }

    /**
     * Demonstrates the basic operations with a block blob.
     *
     * @param container The CloudBlobContainer object to work with
     *
     * @throws Throwable
     * @throws BlobStorageException
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    private static void basicBlockBlobOperations(BlobContainerClient container) throws Throwable, BlobStorageException, IOException, IllegalArgumentException, URISyntaxException {

        // Create sample files for use
        Random random = new Random();

        System.out.println("\tCreating sample files between 128KB-256KB in size for upload demonstration.");
        File tempFile1 = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile1.getAbsolutePath()));
        File tempFile2 = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile2.getAbsolutePath()));
        File tempFile3 = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile3.getAbsolutePath()));

        // Upload a sample file as a block blob
        System.out.println("\n\tUpload a sample file as a block blob.");
        BlobClient blobClient = container.getBlobClient("blockblob1.tmp");
        blobClient.uploadFromFile(tempFile1.getAbsolutePath());
        System.out.println("\t\tSuccessfully uploaded the blob.");

        // Create a read-only snapshot of the blob
        System.out.println("\n\tCreate a read-only snapshot of the blob.");
        BlobClientBase blockBlob1Snapshot = blobClient.createSnapshot();
        System.out.println("\t\tSuccessfully created a snapshot of the blob.");

        // Modify the blob by overwriting it
        System.out.println("\n\tOverwrite the blob by uploading the second sample file.");
        blobClient.uploadFromFile(tempFile2.getAbsolutePath(),true);
        System.out.println("\t\tSuccessfully overwrote the blob.");

        // Acquire a lease on the blob so that another client cannot write to it or delete it
        System.out.println("\n\tAcquiring a lease on the blog to prevent writes and deletes.");
        BlobLeaseClient blockLeaseBlob = new BlobLeaseClientBuilder().blobClient(blobClient).buildClient();
        blockLeaseBlob.acquireLease(30);
        System.out.println(String.format("\t\tSuccessfully acquired a lease on blob %s. Lease state: %s.", blobClient.getBlobName(), blobClient.getProperties().getLeaseStatus().toString()));
        blockLeaseBlob.breakLease();
        System.out.println(String.format("\t\tSuccessfully broke the lease on blob %s. Lease state: %s.", blobClient.getBlobName(), blobClient.getProperties().getLeaseStatus().toString()));

        // Upload a sample file as a block blob using a block list
        System.out.println("\n\tUpload the third sample file as a block blob using a block list.");
        BlockBlobClient blockBlobClient1 = container.getBlobClient("blockblob2.tmp").getBlockBlobClient();
        uploadFileBlocksAsBlockBlob(blockBlobClient1, tempFile3.getAbsolutePath());
        System.out.println("\t\tSuccessfully uploaded the blob using a block list.");

        // Download the block list for the block blob
        System.out.println("\n\tDownload the block list.");
        for (Block blockEntry : blockBlobClient1.listBlocks(BlockListType.COMMITTED).getCommittedBlocks()) {
            System.out.println(String.format("\t\tBlock id: %s (%s), size: %s", blockEntry.getName(), blockEntry.getName(), blockEntry.getSizeLong()));
        }

        // Create sample file for copy demonstration
        System.out.println("\n\tCreating sample file between 10MB-15MB in size for abort copy demonstration.");
        File tempFile4 = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (10 * 1024 * 1024) + random.nextInt(5 * 1024 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile4.getAbsolutePath()));

        // Upload a sample file as a block blob
        System.out.println("\n\tUpload the sample file as a block blob.");
        BlockBlobClient blockBlobClient2 = container.getBlobClient("blockblob3.tmp").getBlockBlobClient();
//        CloudBlockBlob blockBlob3 = container.getBlockBlobReference("blockblob3.tmp");
        blockBlobClient2.upload(new FileInputStream(tempFile4),tempFile4.length());
        System.out.println("\t\tSuccessfully uploaded the blob.");

        // Copy the blob
        System.out.println(String.format("\n\tCopying blob \"%s\".", blockBlobClient2.getBlobUrl()));
        BlockBlobClient blockBlob3Copy = container.getBlobClient(blockBlobClient2.getBlobName() + ".copy").getBlockBlobClient();
        blockBlob3Copy.beginCopy(new BlobBeginCopyOptions(blockBlobClient2.getBlobUrl()));
        waitForCopyToComplete(blockBlob3Copy);
        System.out.println("\t\tSuccessfully copied the blob.");

        // Abort copying the blob
        System.out.println(String.format("\n\tAborting while copying blob \"%s\".", blockBlobClient2.getBlobUrl()));
        BlockBlobClient blockBlob3CopyAborted = container.getBlobClient(blockBlobClient2.getBlobName() + ".copyaborted").getBlockBlobClient();
        boolean copyAborted = true;
        String copyId = blockBlob3CopyAborted.beginCopy(new BlobBeginCopyOptions(blockBlobClient2.getBlobUrl())).poll().getValue().getCopyId();
        try {
            blockBlob3CopyAborted.abortCopyFromUrl(copyId);
        }
        catch (BlobStorageException ex) {
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

        String downloadedBlobPath = String.format("%ssnapshotof-%s", System.getProperty("java.io.tmpdir"), blockBlob1Snapshot.getBlobName());
        System.out.println(String.format("\t\tDownload the blob snapshot from \"%s\" to \"%s\".", blockBlob1Snapshot.getBlobUrl(), downloadedBlobPath));
        blockBlob1Snapshot.downloadToFile(downloadedBlobPath);
        new File(downloadedBlobPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob snapshot.");

        downloadedBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), blobClient.getBlobName());
        System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", blobClient.getBlobUrl(), downloadedBlobPath));
        blobClient.downloadToFile(downloadedBlobPath);
        new File(downloadedBlobPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob.");

        downloadedBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), blockBlobClient1.getBlobName());
        System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", blockBlobClient1.getBlobUrl(), downloadedBlobPath));
        blockBlobClient1.downloadToFile(downloadedBlobPath);
        new File(downloadedBlobPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob.");

        // Delete a blob and its snapshots
        System.out.println(String.format("\n\tDelete the blob \"%s\" its snapshots.", blobClient.getBlobName()));
        blobClient.deleteWithResponse(DeleteSnapshotsOptionType.INCLUDE, null, null, null);
        System.out.println("\t\tSuccessfully deleted the blob and its snapshots.");
    }

    /**
     * Demonstrates the basic operations with a page blob.
     *
     * @param container The CloudBlobContainer object to work with
     *
     * @throws BlobStorageException
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    private static void basicPageBlobOperations(BlobContainerClient container) throws BlobStorageException, IOException, IllegalArgumentException, URISyntaxException {

        // Create sample files for use. We use a file whose size is aligned to 512 bytes since page blobs are expected to be aligned to 512 byte pages.
        System.out.println("\tCreating sample file 128KB in size (aligned to 512 bytes) for upload demonstration.");
        File tempFile = DataGenerator.createTempLocalFile("pageblob-", ".tmp", (128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile.getAbsolutePath()));

        // Upload the sample file sparsely as a page blob (Only upload certain ranges of the file)
        System.out.println("\n\tUpload the sample file sparsely as a page blob.");
        System.out.println("\t\tCreating an empty page blob of the same size as the sample file.");
        PageBlobClient pageBlob = new SpecializedBlobClientBuilder().blobClient(container.getBlobClient("pageblob.tmp")).buildPageBlobClient();
        pageBlob.create(tempFile.length()); // This will throw an IllegalArgumentException if the size if not aligned to 512 bytes.

        // Upload selective pages to the blob
        System.out.println("\t\tUploading selective pages to the blob.");
        FileInputStream tempFileInputStream = null;
        try {
            tempFileInputStream = new FileInputStream(tempFile);
            System.out.println("\t\t\tUploading range start: 0, length: 1024.");
            pageBlob.uploadPages(new PageRange().setStart(0).setEnd(1024),tempFileInputStream);
            System.out.println("\t\t\tUploading range start: 4096, length: 1536.");
            pageBlob.uploadPages(new PageRange().setStart(4096).setEnd(4096 + 1536), tempFileInputStream);
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
        BlobClientBase pageBlobSnapshot = pageBlob.createSnapshot();
        System.out.println("\t\tSuccessfully created a snapshot of the blob.");

        // Upload new pages to the blob, modify and clear existing pages
        System.out.println("\n\tModify the blob by uploading new pages to the blob and clearing existing pages.");
        tempFileInputStream = null;
        try {
            tempFileInputStream = new FileInputStream(tempFile);
            System.out.println("\t\t\tUploading range start: 8192, length: 4096.");
            tempFileInputStream.getChannel().position(8192);
            pageBlob.uploadPages( new PageRange().setStart(8192).setEnd(8192 + 4096), tempFileInputStream);
            System.out.println("\t\t\tClearing range start: 4608, length: 512.");
            pageBlob.clearPages(new PageRange().setStart(4608).setEnd(4608 + 512));
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
        for (PageRange pageRange : pageBlob.getPageRanges(new BlobRange(0)).getPageRange()) {
            System.out.println(String.format("\t\tRange start offset: %d, end offset: %d", pageRange.getStart(), pageRange.getEnd()));
        }

        // Query page range diff between snapshots
        System.out.println("\n\tQuery page range diff between the snapshot and the current state.");
        for (PageRange pageRange : pageBlob.getPageRangesDiff(new BlobRange(0),pageBlobSnapshot.getSnapshotId()).getPageRange()) {
            System.out.println(String.format("\t\tRange start offset: %d, end offset: %d", pageRange.getStart(), pageRange.getEnd()));
        }

        // Download the blob and its snapshot
        System.out.println("\n\tDownload the blob and its snapshot.");

        String downloadedPageBlobSnapshotPath = String.format("%ssnapshotof-%s", System.getProperty("java.io.tmpdir"), pageBlobSnapshot.getBlobName());
        System.out.println(String.format("\t\tDownload the blob snapshot from \"%s\" to \"%s\".", pageBlobSnapshot.getBlobUrl(), downloadedPageBlobSnapshotPath));
        pageBlobSnapshot.downloadToFile(downloadedPageBlobSnapshotPath);
        new File(downloadedPageBlobSnapshotPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob snapshot.");

        String downloadedPageBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), pageBlob.getBlobName());
        System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", pageBlob.getBlobUrl(), downloadedPageBlobPath));
        pageBlob.downloadToFile(downloadedPageBlobPath);
        new File(downloadedPageBlobPath).deleteOnExit();
        System.out.println("\t\t\tSuccessfully downloaded the blob.");
    }

    /**
     * Demonstrates the basic operations with a append blob.
     *
     * @param container The CloudBlobContainer object to work with
     *
     * @throws BlobStorageException
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    private static void basicAppendBlobOperations(BlobContainerClient container) throws BlobStorageException, IOException, IllegalArgumentException, FileNotFoundException {

        // Create sample files for use
        Random random = new Random();
        System.out.println("\tCreating sample files between 128KB-256KB in size for upload demonstration.");
        File tempFile1 = DataGenerator.createTempLocalFile("appendblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile1.getAbsolutePath()));
        File tempFile2 = DataGenerator.createTempLocalFile("appendblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
        System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile2.getAbsolutePath()));

        // Create an append blob and append data to it from the sample file
        System.out.println("\n\tCreate an empty append blob and append data to it from the sample files.");
        AppendBlobClient appendBlob = new SpecializedBlobClientBuilder().blobClient(container.getBlobClient("appendblob.tmp")).buildAppendBlobClient(); ;
        appendBlob.create(true);
        File tempfile = new File(tempFile1.getAbsolutePath());
        appendBlob.appendBlock(new FileInputStream(tempfile),tempfile.length());
        tempfile = new File(tempFile2.getAbsolutePath());
        appendBlob.appendBlock(new FileInputStream(tempfile),tempfile.length());
        System.out.println("\t\tSuccessfully created the append blob and appended data to it.");

        // Write random data blocks to the end of the append blob
        byte[] randomBytes = new byte[4096];
        for (int i = 0; i < 8; i++) {
            random.nextBytes(randomBytes);
            appendBlob.appendBlock(new ByteArrayInputStream(randomBytes), 4096);
        }

        // Download the blob
        if (appendBlob != null) {
            System.out.println("\n\tDownload the blob.");
            String downloadedAppendBlobPath = String.format("%scopyof-%s", System.getProperty("java.io.tmpdir"), appendBlob.getBlobName());
            System.out.println(String.format("\t\tDownload the blob from \"%s\" to \"%s\".", appendBlob.getBlobUrl(), downloadedAppendBlobPath));
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
    private static void uploadFileBlocksAsBlockBlob(BlockBlobClient blockBlob, String filePath) throws Throwable {

        FileInputStream fileInputStream = null;
        ByteArrayInputStream byteInputStream = null;
        byte [] bytes = null;
        try {
            // Open the file
            fileInputStream = new FileInputStream(filePath);
            // Split the file into 32K blocks (block size deliberately kept small for the demo) and upload all the blocks
            int blockNum = 0;
            String blockId = null;
            String blockIdEncoded = null;
            ArrayList<String> blockList = new ArrayList<String>();
            while (fileInputStream.available() > (32 * 1024)) {
                bytes = new byte[32 * 1024];
                fileInputStream.read(bytes);
                byteInputStream = new ByteArrayInputStream(bytes);
                blockId = String.format("%05d", blockNum);
                blockIdEncoded = Base64.getEncoder().encodeToString(blockId.getBytes());
                blockBlob.upload(byteInputStream,32 * 1024, true);
                blockList.add(blockIdEncoded);
                blockNum++;
                System.out.println(bytes.length);
            }
            blockId = String.format("%05d", blockNum);
            blockIdEncoded = Base64.getEncoder().encodeToString(blockId.getBytes());
            bytes = new byte[fileInputStream.available()];
            fileInputStream.read(bytes);
            byteInputStream = new ByteArrayInputStream(bytes);
            blockBlob.upload(byteInputStream,bytes.length,true);
            blockList.add(blockIdEncoded);

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
            if ( byteInputStream != null ) {
                byteInputStream.close();
            }
        }
    }

    /**
     * Wait until the copy complete.
     *
     * @param blob Target of the copy operation
     *
     * @throws InterruptedException
     * @throws BlobStorageException
     */
    private static void waitForCopyToComplete(BlockBlobClient blob) throws InterruptedException, BlobStorageException {
        CopyStatusType copyStatus = CopyStatusType.PENDING;
        while (copyStatus == CopyStatusType.PENDING) {
            Thread.sleep(1000);
//            blob.downloadAttributes();
            copyStatus = blob.getProperties().getCopyStatus();;
        }
    }





}
