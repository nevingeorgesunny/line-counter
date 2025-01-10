package com.nevin.sunny;

import java.io.*;

public class LineCounterByte {

    public static final int BUFFER_SIZE = 16000;
    static String INPUT_FILE = "/Users/nsunny/dev/exp/log-aggregation/1.txt";
    static String outputFile = "/Users/nsunny/dev/exp/log-aggregation/out2.txt";
    private static final int[] PATTERN = {
            0, 0, 0, 0, 45, 0, 0, 45, 0, 0, 32, 0, 0, 58, 0, 0, 58, 0, 0, 46, 0, 0, 0, 43, 0, 0, 0, 0
    };


    static void countLine() throws IOException {

        byte[] buffer = new byte[BUFFER_SIZE];
        int newlineCounter = 0;
        try (
                FileInputStream fileInputStream = new FileInputStream(INPUT_FILE);
                FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
                BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream)
        ) {
            int bytesRead;
            int patternAt = 0;
            boolean startChecking = false;

            while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
                for (int i = 0; i < bytesRead; i++) {

                    // 10 is the ASCII value for a newline character
                    if(i != 0 && buffer[i -1] == 10){
                        //only need to check for pattern after new line
                        startChecking = true;
                    }

                    if (startChecking && matchesPatternAt(buffer[i], PATTERN[patternAt])) {
                        patternAt++;

                        if(patternAt == PATTERN.length){
                            newlineCounter++;
                            patternAt = 0;
                            startChecking = false;
                        }
                    }else {
                        startChecking = false;
                        patternAt = 0;
                    }
                }

                bufferedOutputStream.write(buffer, 0, bytesRead);

            }
            System.out.println("File streamed successfully using a char buffer. total line "+ newlineCounter);
        }
    }

    /**
     * Checks if the current byte matches the specified pattern condition.
     *
     * @param b      The current byte being evaluated.
     * @param patternCondition The condition for this position in the pattern.
     *                         - `0` means the byte must be in the range '0'-'9' (48-57).
     *                         - Any other value means an exact byte match.
     * @return True if the byte satisfies the pattern condition; false otherwise.
     */
    private static boolean matchesPatternAt(byte b, int patternCondition) {
        if (patternCondition == 0) {
            // Condition `0` means the byte must be in the range '0'-'9' (48–57)
            return b >= 48 && b <= 57;
        } else {
            // Otherwise, the byte must match the exact `patternCondition` value
            return b == patternCondition;
        }
    }

}
