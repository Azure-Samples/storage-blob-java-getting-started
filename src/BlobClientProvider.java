import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Manages the storage blob client
 */
public class BlobClientProvider {

    /**
     * Validates the connection string and returns the storage blob client.
     * The connection string must be in the Azure connection string format.
     *
     * @return The newly created CloudBlobClient object
     *
     * @throws IOException
     */
    public static BlobServiceClient getBlobServiceClient() throws IOException {

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
        } catch (IOException ex) {
            System.out.println("\nFailed to load config.properties file.");
            throw ex;
        }

        return new BlobServiceClientBuilder().connectionString(prop.getProperty("StorageConnectionString")).buildClient();
    }

}
