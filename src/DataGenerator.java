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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

/**
 * Random data generator methods.
 */
class DataGenerator {
    /**
     * Creates and returns a randomized name based on the prefix file for use by the sample.
     *
     * @param namePrefix The prefix string to be used in generating the name.
     * @return The randomized name
     */
    static String createRandomName(String namePrefix) {

        return namePrefix + UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * Creates and returns a temporary local file for use by the sample.
     *
     * @param tempFileNamePrefix The prefix string to be used in generating the file's name.
     * @param tempFileNameSuffix The suffix string to be used in generating the file's name.
     * @param bytesToWrite The number of bytes to write to file.
     * @return The newly created File object
     */
    static File createTempLocalFile(String tempFileNamePrefix, String tempFileNameSuffix, int bytesToWrite) throws IOException, IllegalArgumentException{

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
}
