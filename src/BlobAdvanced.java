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

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;

/**
 * This sample illustrates advanced usage of the Azure blob storage service.
 */
class BlobAdvanced {

    /**
     * Executes the samples.
     *
     * @throws URISyntaxException Uri has invalid syntax
     * @throws InvalidKeyException Invalid key
     */
    void runSamples() throws InvalidKeyException, URISyntaxException, IOException {
        System.out.println();
        System.out.println();
        PrintHelper.printSampleStartInfo("Blob Advanced");

        // Create a blob service client
        CloudBlobClient blobClient = BlobClientProvider.getBlobClientReference();

        try {
            System.out.println("List containers sample");
            listContainers(blobClient);
            System.out.println();

            System.out.println("Service properties sample");
            serviceProperties(blobClient);
            System.out.println();

            System.out.println("CORS rules sample");
            corsRules(blobClient);
            System.out.println();

            System.out.println("Container properties sample");
            containerProperties(blobClient);
            System.out.println();

            System.out.println("Container metadata sample");
            containerMetadata(blobClient);
            System.out.println();

            System.out.println("Container Acl sample");
            containerAcl(blobClient);
            System.out.println();

            System.out.println("Blob properties sample");
            blobProperties(blobClient);
            System.out.println();

            System.out.println("Blob metadata sample");
            blobMetadata(blobClient);
            System.out.println();

            // This will fail unless the account is RA-GRS enabled.
//            System.out.println("Service stats sample");
//            serviceStats(blobClient);
//            System.out.println();
        }
        catch (Throwable t) {
            PrintHelper.printException(t);
        }

        PrintHelper.printSampleCompleteInfo("Blob Advanced");
    }

    /**
     * List containers sample.
     * @param blobClient Azure Storage Blob Service
     */
    private void listContainers(CloudBlobClient blobClient) throws URISyntaxException, StorageException {
        ArrayList<String> containerList = new ArrayList<>();
        try {
            System.out.println("Create containers");
            String prefix = UUID.randomUUID().toString();
            for (int i = 0; i < 5; i++) {
                containerList.add(prefix + i);
                blobClient.getContainerReference(prefix + i).create();
            }

            System.out.println("List containers");
            for (final CloudBlobContainer container : blobClient.listContainers(prefix)) {
                System.out.printf("container name: %s%n", container.getName());
            }
        }
        finally {
            System.out.println("Delete containers");
            for (final String containerName : containerList) {
                blobClient.getContainerReference(containerName).deleteIfExists();
            }
        }
    }

    /**
     * Manage the service properties including logging hour and minute metrics and default version.
     * @param blobClient Azure Storage Blob Service
     */
    private void serviceProperties(CloudBlobClient blobClient) throws StorageException {

        System.out.println("Get service properties");
        ServiceProperties originalProps = blobClient.downloadServiceProperties();

        try {
            System.out.println("Set service properties");
            // Change service properties
            ServiceProperties props = new ServiceProperties();
            props.setDefaultServiceVersion(Constants.HeaderConstants.TARGET_STORAGE_VERSION);

            props.setDefaultServiceVersion("2009-09-19");

            props.getLogging().setLogOperationTypes(EnumSet.allOf(LoggingOperations.class));
            props.getLogging().setRetentionIntervalInDays(2);
            props.getLogging().setVersion("1.0");

            final MetricsProperties hours = props.getHourMetrics();
            hours.setMetricsLevel(MetricsLevel.SERVICE_AND_API);
            hours.setRetentionIntervalInDays(1);
            hours.setVersion("1.0");

            final MetricsProperties minutes = props.getMinuteMetrics();
            minutes.setMetricsLevel(MetricsLevel.SERVICE);
            minutes.setRetentionIntervalInDays(1);
            minutes.setVersion("1.0");

            blobClient.uploadServiceProperties(props);

            System.out.println();
            System.out.printf("Default service version: %s%n", props.getDefaultServiceVersion());

            System.out.println("Logging");
            System.out.printf("version: %s%n", props.getLogging().getVersion());
            System.out.printf("retention interval: %d%n", props.getLogging().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getLogging().getLogOperationTypes());
            System.out.println();
            System.out.println("Hour Metrics");
            System.out.printf("version: %s%n", props.getHourMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getHourMetrics().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getHourMetrics().getMetricsLevel());
            System.out.println();
            System.out.println("Minute Metrics");
            System.out.printf("version: %s%n", props.getMinuteMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getMinuteMetrics().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getMinuteMetrics().getMetricsLevel());
            System.out.println();
        }
        finally{
            // Revert back to original service properties
            blobClient.uploadServiceProperties(originalProps);
        }
    }

    /**
     * Set CORS rules sample.
     * @param blobClient Azure Storage Blob Service
     */
    private void corsRules(CloudBlobClient blobClient) throws StorageException {

        ServiceProperties originalProperties = blobClient.downloadServiceProperties();

        try {
            // Set CORS rules
            System.out.println("Set CORS rules");
            CorsRule ruleAllowAll = new CorsRule();
            ruleAllowAll.getAllowedOrigins().add("*");
            ruleAllowAll.getAllowedMethods().add(CorsHttpMethods.GET);
            ruleAllowAll.getAllowedHeaders().add("*");
            ruleAllowAll.getExposedHeaders().add("*");
            ServiceProperties props = blobClient.downloadServiceProperties();
            props.getCors().getCorsRules().add(ruleAllowAll);
            blobClient.uploadServiceProperties(props);
        }
        finally {
            // Revert back to original service properties
            blobClient.uploadServiceProperties(originalProperties);
        }
    }

    /**
     * Manage container properties
     * @param blobClient Azure Storage Blob Service
     */
    private void containerProperties(CloudBlobClient blobClient) throws URISyntaxException, StorageException {
        // Get a reference to a container
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));
        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            container.createIfNotExists();

            System.out.println("Get container properties");
            BlobContainerProperties properties = container.getProperties();
            System.out.printf("Etag: %s%n", properties.getEtag());
            System.out.printf("Last modified: %s%n", properties.getLastModified());
            System.out.printf("Lease state: %s%n", properties.getLeaseState());
            System.out.printf("Lease status: %s%n", properties.getLeaseStatus());
        }
        finally {
            if (container.deleteIfExists()) {
                System.out.println(String.format("Successfully deleted the container: %s", container.getName()));
            }
        }
    }

    /**
     * Manage container metadata
     * @param blobClient Azure Storage Blob Service
     */
    private void containerMetadata(CloudBlobClient blobClient) throws URISyntaxException, StorageException {
        // Get a reference to a container
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));
        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            container.createIfNotExists();

            System.out.println("Set container metadata");
            container.getMetadata().put("key1", "value1");
            container.getMetadata().put("foo", "bar");
            container.uploadMetadata();

            System.out.println("Get container metadata:");
            HashMap<String, String> metadata = container.getMetadata();
            Iterator it = metadata.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.printf(" %s = %s%n", pair.getKey(), pair.getValue());
                it.remove();
            }
        }
        finally {
            if (container.deleteIfExists()) {
                System.out.println(String.format("Successfully deleted the container: %s", container.getName()));
            }
        }
    }

    /**
     * Manage container access properties
     * @param blobClient Azure Storage Blob Service
     */
    private void containerAcl(CloudBlobClient blobClient) throws StorageException, URISyntaxException, InterruptedException {
        // Get a reference to a container
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            container.createIfNotExists();

            // Get permissions
            BlobContainerPermissions permissions = container.downloadPermissions();

            System.out.println("Set container permissions");
            final Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            final Date start = cal.getTime();
            cal.add(Calendar.MINUTE, 30);
            final Date expiry = cal.getTime();

            permissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
            policy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.LIST, SharedAccessBlobPermissions.CREATE));
            policy.setSharedAccessStartTime(start);
            policy.setSharedAccessExpiryTime(expiry);
            permissions.getSharedAccessPolicies().put("key1", policy);

            // Set container permissions
            container.uploadPermissions(permissions);
            System.out.println("Wait 30 seconds for the container permissions to take effect");
            Thread.sleep(30000);

            System.out.println("Get container permissions");
            // Get container permissions
            permissions = container.downloadPermissions();

            System.out.printf(" Public access: %s%n", permissions.getPublicAccess());
            HashMap<String, SharedAccessBlobPolicy> accessPolicies = permissions.getSharedAccessPolicies();
            Iterator it = accessPolicies.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                SharedAccessBlobPolicy value = (SharedAccessBlobPolicy) pair.getValue();
                System.out.printf(" %s: %n", pair.getKey());
                System.out.printf("  Permissions: %s%n", value.permissionsToString());
                System.out.printf("  Start: %s%n", value.getSharedAccessStartTime());
                System.out.printf("  Expiry: %s%n", value.getSharedAccessStartTime());
                it.remove();
            }

            System.out.println("Clear container permissions");
            // Clear permissions
            permissions.getSharedAccessPolicies().clear();
            container.uploadPermissions(permissions);
        }
        finally {
            if (container.deleteIfExists()) {
                System.out.println(String.format("Successfully deleted the container: %s", container.getName()));
            }
        }
    }

    /**
     * Mangage blob properties
     * @param blobClient Azure Storage Blob Service
     */
    private void blobProperties(CloudBlobClient blobClient) throws StorageException, URISyntaxException, IOException {
        // Get a reference to a container
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            container.createIfNotExists();

            Random random = new Random();
            File tempFile = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
            System.out.println(String.format("Successfully created the file \"%s\"", tempFile.getAbsolutePath()));

            System.out.println("Use a sample file as a block blob");
            CloudBlockBlob blob = container.getBlockBlobReference("blockblob1.tmp");

            // Set blob properties
            System.out.println("Set blob properties");
            blob.getProperties().setContentType("text/plain");
            blob.getProperties().setContentEncoding("UTF8");
            blob.getProperties().setContentLanguage("en");

            // Upload the block blob
            blob.uploadFromFile(tempFile.getAbsolutePath());
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
        }
        finally {
            if (container.deleteIfExists()) {
                System.out.println(String.format("Successfully deleted the container: %s", container.getName()));
            }
        }
    }

    /**
     * Manage the blob metadata
     * @param blobClient Azure Storage Blob Service
     */
    private void blobMetadata(CloudBlobClient blobClient) throws URISyntaxException, StorageException, IOException {
        // Get a reference to a container
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            container.createIfNotExists();

            Random random = new Random();
            File tempFile = DataGenerator.createTempLocalFile("blockblob-", ".tmp", (128 * 1024) + random.nextInt(128 * 1024));
            System.out.println(String.format("Successfully created the file \"%s\"", tempFile.getAbsolutePath()));

            // Use a sample file as a block blob
            System.out.println("Upload a sample file as a block blob");
            CloudBlockBlob blob = container.getBlockBlobReference("blockblob1.tmp");

            System.out.println("Set blob metadata");
            blob.getMetadata().put("key1", "value1");
            blob.getMetadata().put("foo", "bar");

            // Upload the block blob
            blob.uploadFromFile(tempFile.getAbsolutePath());
            System.out.println("Successfully uploaded the blob");

            System.out.println("Get blob metadata:");
            HashMap<String, String> metadata = blob.getMetadata();
            Iterator it = metadata.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.printf(" %s = %s%n", pair.getKey(), pair.getValue());
                it.remove();
            }
        }
        finally {
            if (container.deleteIfExists()) {
                System.out.println(String.format("Successfully deleted the container: %s", container.getName()));
            }
        }
    }

    /**
     * Retrieve statistics related to replication for the Blob service.
     * This operation is only available on the secondary location endpoint
     * when read-access geo-redundant replication is enabled for the storage account.
     * @param blobClient Azure Storage Blob Service
     */
    private void serviceStats(CloudBlobClient blobClient) throws StorageException {
        // Get service stats
        System.out.println("Service Stats:");
        ServiceStats stats = blobClient.getServiceStats();
        System.out.printf("- status: %s%n", stats.getGeoReplication().getStatus());
        System.out.printf("- last sync time: %s%n", stats.getGeoReplication().getLastSyncTime());
    }

}
