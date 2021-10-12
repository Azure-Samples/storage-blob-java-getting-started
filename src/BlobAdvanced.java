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
import com.azure.storage.blob.models.BlobAccessPolicy;
import com.azure.storage.blob.models.BlobAnalyticsLogging;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerProperties;
import com.azure.storage.blob.models.BlobCorsRule;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobMetrics;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRetentionPolicy;
import com.azure.storage.blob.models.BlobServiceProperties;
import com.azure.storage.blob.models.BlobServiceStatistics;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobContainersOptions;
import com.azure.storage.blob.models.PublicAccessType;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.common.implementation.Constants;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * This sample illustrates advanced usage of the Azure blob storage service.
 */
class BlobAdvanced {

    /**
     * Executes the samples.
     */
    void runSamples() {
        System.out.println();
        System.out.println();
        PrintHelper.printSampleStartInfo("Blob Advanced");

        try {
            // Create a blob service client
            BlobServiceClient blobServiceClient = BlobClientProvider.getBlobServiceClient();

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
        } catch (Exception ex) {
            PrintHelper.printException(ex);
        }

        PrintHelper.printSampleCompleteInfo("Blob Advanced");
    }

    /**
     * List containers sample.
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void listContainers(BlobServiceClient blobServiceClient) {
        ArrayList<String> containerList = new ArrayList<>();
        try {
            System.out.println("Create containers");
            String prefix = UUID.randomUUID().toString();
            for (int i = 0; i < 5; i++) {
                containerList.add(prefix + i);
                blobServiceClient.createBlobContainer(prefix + i);
            }

            System.out.println("List containers");
            for (final BlobContainerItem blobContainerItem : blobServiceClient.listBlobContainers(new ListBlobContainersOptions().setPrefix(prefix), (Duration) null)) {
                System.out.printf("container name: %s%n", blobContainerItem.getName());
            }
        } finally {
            System.out.println("Delete containers");
            for (final String containerName : containerList) {
                try {
                    blobServiceClient.getBlobContainerClient(containerName).delete();
                } catch (Exception ex) {
                    if (!(ex instanceof BlobStorageException)
                            || !BlobErrorCode.CONTAINER_NOT_FOUND.equals(((BlobStorageException)ex).getErrorCode())) {
                        throw ex;
                    }
                }
            }
        }
    }

    /**
     * Manage the service properties including logging hour and minute metrics and default version.
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void serviceProperties(BlobServiceClient blobServiceClient) {

        System.out.println("Get service properties");
        BlobServiceProperties originalProps = blobServiceClient.getProperties();

        try {
            System.out.println("Set service properties");
            // Change service properties
            BlobServiceProperties props = blobServiceClient.getProperties();
            props.setDefaultServiceVersion(Constants.HeaderConstants.TARGET_STORAGE_VERSION);

            props.setDefaultServiceVersion("2009-09-19");

            props.setDeleteRetentionPolicy(new BlobRetentionPolicy().setEnabled(true).setDays(3));

            props.setLogging(new BlobAnalyticsLogging()
                    .setDelete(true)
                    .setRead(true)
                    .setWrite(true)
                    .setVersion("1.0")
                    .setRetentionPolicy(new BlobRetentionPolicy().setEnabled(true).setDays(3)));

            props.setHourMetrics(new BlobMetrics()
                    .setIncludeApis(true)
                    .setRetentionPolicy(new BlobRetentionPolicy().setDays(1))
                    .setVersion("1.0")
                    .setRetentionPolicy(new BlobRetentionPolicy().setEnabled(true).setDays(3)));

            props.setMinuteMetrics(new BlobMetrics()
                    .setIncludeApis(false)
                    .setRetentionPolicy(new BlobRetentionPolicy().setDays(1))
                    .setVersion("1.0")
                    .setRetentionPolicy(new BlobRetentionPolicy().setEnabled(true).setDays(3)));

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
            blobServiceClient.setProperties(originalProps);
        }
    }

    /**
     * Set CORS rules sample.
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void corsRules(BlobServiceClient blobServiceClient) {

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
    private void containerProperties(BlobServiceClient blobServiceClient) {
        // Get a reference to a container
        // The container name must be lower case
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));
        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            try {
                containerClient.create();
            } catch (Exception ex) {
                if (!(ex instanceof BlobStorageException)
                        || !BlobErrorCode.CONTAINER_ALREADY_EXISTS.equals(((BlobStorageException)ex).getErrorCode())) {
                    throw ex;
                }
            }

            System.out.println("Get container properties");
            BlobContainerProperties properties = containerClient.getProperties();
            System.out.printf("Etag: %s%n", properties.getETag());
            System.out.printf("Last modified: %s%n", properties.getLastModified());
            System.out.printf("Lease state: %s%n", properties.getLeaseState());
            System.out.printf("Lease status: %s%n", properties.getLeaseStatus());
        } finally {
            containerClient.delete();
            System.out.println(String.format("Successfully deleted the container: %s", containerClient.getBlobContainerName()));
        }
    }

    /**
     * Manage container metadata
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void containerMetadata(BlobServiceClient blobServiceClient) {
        // Get a reference to a container
        // The container name must be lower case
        String containerName = "blobadvancedcontainer" + UUID.randomUUID().toString().replace("-", "");
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            try {
                containerClient.create();
            } catch (Exception ex) {
                if (!(ex instanceof BlobStorageException)
                        || !BlobErrorCode.CONTAINER_ALREADY_EXISTS.equals(((BlobStorageException)ex).getErrorCode())) {
                    throw ex;
                }
            }
            System.out.println("Set container metadata");
            Map<String, String> metadataMap = containerClient.getProperties().getMetadata();
            metadataMap = metadataMap != null ? metadataMap : new HashMap();
            metadataMap.put("key1", "value1");
            metadataMap.put("foo", "bar");
            containerClient.setMetadata(metadataMap);
            System.out.println("Get container metadata:");
            containerClient.getProperties().getMetadata().entrySet().forEach(pair -> {
                System.out.printf(" %s = %s%n", pair.getKey(), pair.getValue());
            });
        } finally {
            containerClient.delete();
            System.out.println(String.format("Successfully deleted the container: %s", containerClient.getBlobContainerName()));
        }
    }

    /**
     * Manage container access properties
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void containerAcl(BlobServiceClient blobServiceClient) throws InterruptedException {
        // Get a reference to a container
        // The container name must be lower case
        String containerName = "blobadvancedcontainer" + UUID.randomUUID().toString().replace("-", "");
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            try {
                containerClient.create();
            } catch (Exception ex) {
                if (!(ex instanceof BlobStorageException)
                        || !BlobErrorCode.CONTAINER_ALREADY_EXISTS.equals(((BlobStorageException)ex).getErrorCode())) {
                    throw ex;
                }
            }

            System.out.println("Set container permissions");
            OffsetDateTime startOn = OffsetDateTime.now(ZoneOffset.UTC);
            OffsetDateTime expiresOn = startOn.plusMinutes(30);
            containerClient.getAccessPolicy().getIdentifiers().forEach(blobSignedIdentifier -> {
                blobSignedIdentifier.setAccessPolicy(new BlobAccessPolicy().setStartsOn(startOn).setExpiresOn(expiresOn).setPermissions("lc"));
            });
            containerClient.setAccessPolicy(PublicAccessType.CONTAINER, containerClient.getAccessPolicy().getIdentifiers());

            System.out.println("Wait 30 seconds for the container permissions to take effect");
            Thread.sleep(30000);

            System.out.println("Get container permissions");
            // Get container permissions
            System.out.printf(" Public access: %s%n", containerClient.getAccessPolicy().getBlobAccessType());
            containerClient.getAccessPolicy().getIdentifiers().forEach(blobSignedIdentifier -> {
                System.out.printf("  Permissions: %s%n", blobSignedIdentifier.getAccessPolicy().getPermissions());
                System.out.printf("  Start: %s%n", blobSignedIdentifier.getAccessPolicy().getStartsOn());
                System.out.printf("  Expiry: %s%n", blobSignedIdentifier.getAccessPolicy().getExpiresOn());
                // Clear permissions
                blobSignedIdentifier.setAccessPolicy(new BlobAccessPolicy());
            });
            System.out.println("Clear container permissions");
            containerClient.setAccessPolicy(PublicAccessType.CONTAINER, containerClient.getAccessPolicy().getIdentifiers());
        } finally {
            containerClient.delete();
            System.out.println(String.format("Successfully deleted the container: %s", containerClient.getBlobContainerName()));
        }
    }

    /**
     * Mangage blob properties
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void blobProperties(BlobServiceClient blobServiceClient) throws IOException {
        // Get a reference to a container
        // The container name must be lower case
        String containerName = "blobadvancedcontainer" + UUID.randomUUID().toString().replace("-", "");
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            try {
                containerClient.create();
            } catch (Exception ex) {
                if (!(ex instanceof BlobStorageException)
                        || !BlobErrorCode.CONTAINER_ALREADY_EXISTS.equals(((BlobStorageException)ex).getErrorCode())) {
                    throw ex;
                }
            }

            Random random = new Random();
            File tempFile = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
            System.out.println(String.format("Successfully created the file \"%s\"", tempFile.getAbsolutePath()));

            System.out.println("Use a sample file as a block blob");
            BlobClient blob = containerClient.getBlobClient("blockblob1.tmp");

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
            containerClient.delete();
            System.out.println(String.format("Successfully deleted the container: %s", containerClient.getBlobContainerName()));
        }
    }

    /**
     * Manage the blob metadata
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void blobMetadata(BlobServiceClient blobServiceClient) throws IOException {
        // Get a reference to a container
        // The container name must be lower case
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            try {
                containerClient.create();
            } catch (Exception ex) {
                if (!(ex instanceof BlobStorageException)
                        || !BlobErrorCode.CONTAINER_ALREADY_EXISTS.equals(((BlobStorageException)ex).getErrorCode())) {
                    throw ex;
                }
            }

            Random random = new Random();
            File tempFile = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
            System.out.println(String.format("Successfully created the file \"%s\"", tempFile.getAbsolutePath()));

            // Use a sample file as a block blob
            System.out.println("Upload a sample file as a block blob");
            BlobClient blob = containerClient.getBlobClient("blockblob1.tmp");

            System.out.println("Set blob metadata");
            Map<String, String> metadata = blob.exists() ? blob.getProperties().getMetadata() : new HashMap();
            metadata = metadata != null ? metadata : new HashMap();
            metadata.put("key1", "value1");
            metadata.put("foo", "bar");

            // Upload the block blob
            blob.uploadFromFileWithResponse(new BlobUploadFromFileOptions(tempFile.getAbsolutePath()).setMetadata(metadata), Duration.ofSeconds(30), Context.NONE);
            System.out.println("Successfully uploaded the blob");

            System.out.println("Get blob metadata:");
            blob.getProperties().getMetadata().entrySet()
                    .forEach(pair -> System.out.printf(" %s = %s%n", pair.getKey(), pair.getValue()));
        } finally {
            containerClient.delete();
            System.out.println(String.format("Successfully deleted the container: %s", containerClient.getBlobContainerName()));
        }
    }

    /**
     * Retrieve statistics related to replication for the Blob service.
     * This operation is only available on the secondary location endpoint
     * when read-access geo-redundant replication is enabled for the storage account.
     *
     * @param blobServiceClient Azure Storage Blob Service
     */
    private void serviceStats(BlobServiceClient blobServiceClient) {
        // Get service stats
        System.out.println("Service Stats:");
        BlobServiceStatistics stats = blobServiceClient.getStatistics();
        System.out.printf("- status: %s%n", stats.getGeoReplication().getStatus());
        System.out.printf("- last sync time: %s%n", stats.getGeoReplication().getLastSyncTime());
    }

}
