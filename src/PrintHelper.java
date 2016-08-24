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


import com.microsoft.azure.storage.StorageException;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * A class which provides utility methods
 * 
 */
public final class PrintHelper {

    /**
     * Prints out the sample start information .
     */
    public static void printSampleStartInfo(String sampleName) {
        System.out.println(String.format(
                "%s samples starting...",
                sampleName));
    }

    /**
     * Prints out the sample complete information .
     */
    public static void printSampleCompleteInfo(String sampleName) {
        System.out.println(String.format(
                "%s samples completed.",
                sampleName));
    }

    /**
     * Print the exception stack trace
     *
     * @param t Exception to be printed
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
