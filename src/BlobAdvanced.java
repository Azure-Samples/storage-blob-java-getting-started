/*
  Copyright Microsoft Corporation

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.common.implementation.Constants;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * This sample illustrates advanced usage of the Azure blob storage service.
 */
class BlobAdvanced {

    /**
     * Executes the samples.
     *
     * @throws URISyntaxException  Uri has invalid syntax
     * @throws InvalidKeyException Invalid key
     */
    void runSamples() throws InvalidKeyException, URISyntaxException, IOException {
        System.out.println();
        System.out.println();
        PrintHelper.printSampleStartInfo("Blob Advanced");

        // Create a blob service client
        BlobServiceClient blobServiceClient = BlobClientProvider.getBlobClientReference();

        try {
            System.out.println("List containers sample");
            listContainers(blobServiceClient);
            System.out.println();

            System.out.println("Service properties sample");
            serviceProperties(blobServiceClient);
            System.out.println();

            System.out.println("CORS rules sample");
            corsRules(blobServiceClient);
            System.out.println();

            System.out.println("Container properties sample");
            containerProperties(blobServiceClient);
            System.out.println();

            System.out.println("Container metadata sample");
            containerMetadata(blobServiceClient);
            System.out.println();

            System.out.println("Container Acl sample");
            containerAcl(blobServiceClient);
            System.out.println();

            System.out.println("Blob properties sample");
            blobProperties(blobServiceClient);
            System.out.println();

            System.out.println("Blob metadata sample");
            blobMetadata(blobServiceClient);
            System.out.println();

            // This will fail unless the account is RA-GRS enabled.
//            System.out.println("Service stats sample");
//            serviceStats(blobClient);
//            System.out.println();
        } catch (Throwable t) {
            PrintHelper.printException(t);
        }

        PrintHelper.printSampleCompleteInfo("Blob Advanced");
    }

    /**
     * List containers sample.
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void listContainers(BlobServiceClient blobServiceClient) throws URISyntaxException, BlobStorageException {
        ArrayList<String> containerList = new ArrayList<>();
        try {
            System.out.println("Create containers");
            String prefix = UUID.randomUUID().toString();
            for (int i = 0; i < 5; i++) {
                containerList.add(prefix + i);
                blobServiceClient.createBlobContainer(prefix + i);
            }

            System.out.println("List containers");
            for (final BlobContainerItem container : blobServiceClient.listBlobContainers(new ListBlobContainersOptions().setPrefix(prefix),(Duration)null)) {
                System.out.printf("container name: %s%n", container.getName());
            }
        } finally {
            System.out.println("Delete containers");
            for (final String containerName : containerList) {
                blobServiceClient.getBlobContainerClient(containerName).delete();
            }
        }
    }

    /**
     * Manage the service properties including logging hour and minute metrics and default version.
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void serviceProperties(BlobServiceClient blobServiceClient) throws BlobStorageException {

        System.out.println("Get service properties");
        BlobServiceProperties originalProps = blobServiceClient.getProperties();

        try {
            System.out.println("Set service properties");
            // Change service properties
            BlobServiceProperties props = blobServiceClient.getProperties();
            props.setDefaultServiceVersion(Constants.HeaderConstants.TARGET_STORAGE_VERSION);

            props.setDefaultServiceVersion("2009-09-19");

            props.setDeleteRetentionPolicy(new BlobRetentionPolicy().setEnabled(true).setDays(3));

            final BlobAnalyticsLogging logging = props.getLogging() != null ? props.getLogging() : new BlobAnalyticsLogging();
            props.setLogging(logging.setDelete(true).setRead(true).setWrite(true).setVersion("1.0").setRetentionPolicy(new BlobRetentionPolicy().setEnabled(true).setDays(3)));
            final BlobMetrics hours = props.getHourMetrics() != null ? props.getHourMetrics() : new BlobMetrics();
            props.setHourMetrics(hours.setIncludeApis(true).setRetentionPolicy(new BlobRetentionPolicy().setDays(1)).setVersion("1.0").setRetentionPolicy(new BlobRetentionPolicy().setEnabled(true).setDays(3)));
            final BlobMetrics minutes = props.getMinuteMetrics() != null ? props.getMinuteMetrics() : new BlobMetrics();
            props.setMinuteMetrics(minutes.setIncludeApis(false).setRetentionPolicy(new BlobRetentionPolicy().setDays(1)).setVersion("1.0").setRetentionPolicy(new BlobRetentionPolicy().setEnabled(true).setDays(3)));

            blobServiceClient.setProperties(props);

            System.out.println();
            System.out.printf("Default service version: %s%n", props.getDefaultServiceVersion());

            System.out.println("Logging");
            System.out.printf("version: %s%n", props.getLogging().getVersion());
            System.out.printf("retention interval: %d%n", props.getLogging().getRetentionPolicy().getDays());
            System.out.printf("operation delete types: %s%n", props.getLogging().isDelete());
            System.out.printf("operation read types: %s%n", props.getLogging().isRead());
            System.out.printf("operation write types: %s%n", props.getLogging().isWrite());
            System.out.println();
            System.out.println("Hour Metrics");
            System.out.printf("version: %s%n", props.getHourMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getHourMetrics().getRetentionPolicy().getDays());
            System.out.printf("operation include apis types: %s%n", props.getHourMetrics().isIncludeApis());
            System.out.println();
            System.out.println("Minute Metrics");
            System.out.printf("version: %s%n", props.getMinuteMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getHourMetrics().getRetentionPolicy().getDays());
            System.out.printf("operation include apis types: %s%n", props.getMinuteMetrics().isIncludeApis());
            System.out.println();
        } finally {
            // Revert back to original service properties
            blobServiceClient.setPropertiesWithResponse(originalProps, Duration.ofSeconds(60), Context.NONE);
        }
    }

    /**
     * Set CORS rules sample.
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void corsRules(BlobServiceClient blobServiceClient) throws BlobStorageException {

        BlobServiceProperties originalProperties = blobServiceClient.getProperties();

        try {
            // Set CORS rules
            System.out.println("Set CORS rules");
            BlobCorsRule ruleAllowAll = new BlobCorsRule();
            ruleAllowAll.setAllowedOrigins("*");
            ruleAllowAll.setAllowedMethods("GET");
            ruleAllowAll.setAllowedHeaders("*");
            ruleAllowAll.setExposedHeaders("*");
            BlobServiceProperties props = blobServiceClient.getProperties();
            props.getCors().add(ruleAllowAll);
            blobServiceClient.setPropertiesWithResponse(props, Duration.ofSeconds(60), Context.NONE);
        } finally {
            // Revert back to original service properties
            blobServiceClient.setPropertiesWithResponse(originalProperties, Duration.ofSeconds(60), Context.NONE);
        }
    }

    /**
     * Manage container properties
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void containerProperties(BlobServiceClient blobServiceClient) throws URISyntaxException, BlobStorageException {
        // Get a reference to a container
        // The container name must be lower case
        BlobContainerClient container = blobServiceClient.getBlobContainerClient("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));
        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            if (!container.exists()) {
                container.create();
            }

            System.out.println("Get container properties");
            BlobContainerProperties properties = container.getProperties();
            System.out.printf("Etag: %s%n", properties.getETag());
            System.out.printf("Last modified: %s%n", properties.getLastModified());
            System.out.printf("Lease state: %s%n", properties.getLeaseState());
            System.out.printf("Lease status: %s%n", properties.getLeaseStatus());
        } finally {
            if (container.exists()) {
                container.delete();
                System.out.println(String.format("Successfully deleted the container: %s", container.getBlobContainerName()));
            }
        }
    }

    /**
     * Manage container metadata
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void containerMetadata(BlobServiceClient blobServiceClient) throws URISyntaxException, BlobStorageException {
        // Get a reference to a container
        // The container name must be lower case
        String containerName = "blobadvancedcontainer" + UUID.randomUUID().toString().replace("-", "");
        BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerName);
        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            if (!container.exists()) {
                container.create();
            }
            System.out.println("Set container metadata");
            HashMap<String,String> metadataMap = container.getProperties().getMetadata() != null ? (HashMap)container.getProperties().getMetadata() : new HashMap<String,String>();
            metadataMap.put("key1", "value1");
            metadataMap.put("foo", "bar");
            container.setMetadata(metadataMap);
            System.out.println("Get container metadata:");
            Map<String, String> metadata = container.getProperties().getMetadata();
            Iterator it = metadata.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.printf(" %s = %s%n", pair.getKey(), pair.getValue());
                it.remove();
            }
        } finally {
            if (container.exists()) {
                container.delete();
                System.out.println(String.format("Successfully deleted the container: %s", container.getBlobContainerName()));
            }
        }
    }

    /**
     * Manage container access properties
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void containerAcl(BlobServiceClient blobServiceClient) throws BlobStorageException, URISyntaxException, InterruptedException {
        // Get a reference to a container
        // The container name must be lower case
        String containerName = "blobadvancedcontainer" + UUID.randomUUID().toString().replace("-", "");
        BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerName);

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            if (!container.exists()) {
                container.create();
            }

            System.out.println("Set container permissions");
            OffsetDateTime startOn = OffsetDateTime.now(ZoneOffset.UTC);
            OffsetDateTime expiresOn = startOn.plusMinutes(30);
            container.getAccessPolicy().getIdentifiers().forEach(blobSignedIdentifier -> {
                blobSignedIdentifier.setAccessPolicy(new BlobAccessPolicy().setStartsOn(startOn).setExpiresOn(expiresOn).setPermissions("lc"));
            });
            container.setAccessPolicy(PublicAccessType.CONTAINER, container.getAccessPolicy().getIdentifiers());

            System.out.println("Wait 30 seconds for the container permissions to take effect");
            Thread.sleep(30000);

            System.out.println("Get container permissions");
            // Get container permissions
            System.out.printf(" Public access: %s%n", container.getAccessPolicy().getBlobAccessType());
            container.getAccessPolicy().getIdentifiers().forEach(blobSignedIdentifier -> {
                System.out.printf("  Permissions: %s%n", blobSignedIdentifier.getAccessPolicy().getPermissions());
                System.out.printf("  Start: %s%n", blobSignedIdentifier.getAccessPolicy().getStartsOn());
                System.out.printf("  Expiry: %s%n", blobSignedIdentifier.getAccessPolicy().getExpiresOn());
                // Clear permissions
                blobSignedIdentifier.setAccessPolicy(new BlobAccessPolicy());
            });
            System.out.println("Clear container permissions");
            container.setAccessPolicy(PublicAccessType.CONTAINER, container.getAccessPolicy().getIdentifiers());
        } finally {
            if (container.exists()) {
                container.delete();
                System.out.println(String.format("Successfully deleted the container: %s", container.getBlobContainerName()));
            }
        }
    }

    /**
     * Mangage blob properties
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void blobProperties(BlobServiceClient blobServiceClient) throws BlobStorageException, URISyntaxException, IOException {
        // Get a reference to a container
        // The container name must be lower case
        String containerName = "blobadvancedcontainer" + UUID.randomUUID().toString().replace("-", "");
        BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerName);

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            if (!container.exists()) {
                container.create();
            }

            Random random = new Random();
            File tempFile = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
            System.out.println(String.format("Successfully created the file \"%s\"", tempFile.getAbsolutePath()));

            System.out.println("Use a sample file as a block blob");
            BlobClient blob = container.getBlobClient("blockblob1.tmp");

            // Set blob properties
            System.out.println("Set blob properties");
            BlobHttpHeaders blobHeaders = new BlobHttpHeaders().setContentLanguage("en").setContentEncoding("UTF8").setContentType("text/plain");
            // Upload the block blob
            blob.uploadFromFileWithResponse(new BlobUploadFromFileOptions(tempFile.getAbsolutePath()).setHeaders(blobHeaders), Duration.ofSeconds(30), Context.NONE);
            System.out.println("Successfully uploaded the blob");

            System.out.println("Get blob properties");
            BlobProperties properties = blob.getProperties();
            System.out.printf("Blob type: %s%n", properties.getBlobType());
            System.out.printf("Cache control: %s%n", properties.getCacheControl());
            System.out.printf("Content disposition: %s%n", properties.getContentDisposition());
            System.out.printf("Content encoding: %s%n", properties.getContentEncoding());
            System.out.printf("Content language: %s%n", properties.getContentLanguage());
            System.out.printf("Content type: %s%n", properties.getContentType());
            System.out.printf("Last modified: %s%n", properties.getLastModified());
            System.out.printf("Lease state: %s%n", properties.getLeaseState());
            System.out.printf("Lease status: %s%n", properties.getLeaseStatus());
        } finally {
            if (container.exists()) {
                container.delete();
                System.out.println(String.format("Successfully deleted the container: %s", container.getBlobContainerName()));
            }
        }
    }

    /**
     * Manage the blob metadata
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void blobMetadata(BlobServiceClient blobServiceClient) throws URISyntaxException, BlobStorageException, IOException {
        // Get a reference to a container
        // The container name must be lower case
        BlobContainerClient container = blobServiceClient.getBlobContainerClient("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            if (!container.exists()) {
                container.create();
            }

            Random random = new Random();
            File tempFile = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
            System.out.println(String.format("Successfully created the file \"%s\"", tempFile.getAbsolutePath()));

            // Use a sample file as a block blob
            System.out.println("Upload a sample file as a block blob");
            BlobClient blob = container.getBlobClient("blockblob1.tmp");

            System.out.println("Set blob metadata");
            Map<String,String> metadata = blob.exists() && blob.getProperties().getMetadata() != null ? blob.getProperties().getMetadata() : new HashMap();
            metadata.put("key1", "value1");
            metadata.put("foo", "bar");

            // Upload the block blob
            blob.uploadFromFileWithResponse(new BlobUploadFromFileOptions(tempFile.getAbsolutePath()).setMetadata(metadata),Duration.ofSeconds(30),Context.NONE);
            System.out.println("Successfully uploaded the blob");

            System.out.println("Get blob metadata:");
            metadata = (HashMap) blob.getProperties().getMetadata();
            Iterator it = metadata.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.printf(" %s = %s%n", pair.getKey(), pair.getValue());
                it.remove();
            }
        } finally {
            if (container.exists()) {
                container.delete();
                System.out.println(String.format("Successfully deleted the container: %s", container.getBlobContainerName()));
            }
        }
    }

    /**
     * Retrieve statistics related to replication for the Blob service.
     * This operation is only available on the secondary location endpoint
     * when read-access geo-redundant replication is enabled for the storage account.
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void serviceStats(BlobServiceClient blobServiceClient) throws BlobStorageException {
        // Get service stats
        System.out.println("Service Stats:");
        BlobServiceStatistics stats = blobServiceClient.getStatistics();
        System.out.printf("- status: %s%n", stats.getGeoReplication().getStatus());
        System.out.printf("- last sync time: %s%n", stats.getGeoReplication().getLastSyncTime());
    }

}
