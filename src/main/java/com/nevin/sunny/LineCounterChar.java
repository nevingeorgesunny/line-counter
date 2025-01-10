package com.nevin.sunny;

import java.io.*;

public class LineCounterChar {

    public static final int BUFFER_SIZE = 16000;
    static String INPUT_FILE = "/Users/nsunny/dev/exp/log-aggregation/1.txt";
    static String outputFile = "/Users/nsunny/dev/exp/log-aggregation/out2.txt";
    private static final int DELIMITER_LENGTH = 28;
    private static final char[] PATTERN = {
            0, 0, 0, 0, '-', 0, 0, '-', 0, 0, ' ', 0, 0, ':', 0, 0, ':', 0, 0, '.', 0, 0, 0, '+', 0, 0, 0, 0
    };

    static void countLine() throws IOException {

        char[] buffer = new char[BUFFER_SIZE];
        int newlineCounter = 0;
        try (
                FileReader fr = new FileReader(INPUT_FILE);
                FileWriter fw = new FileWriter(outputFile)
        ) {
            int patternAt = 0;
            int charsRead;
            boolean startChecking = true;

            while ((charsRead = fr.read(buffer)) != -1) {
                for (int i = 0; i < charsRead; i++) {

                    if(i != 0 && buffer[i -1] == '\n'){
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
                fw.write(buffer, 0, charsRead); // Write only the characters read in this iteration
            }
            System.out.println("File streamed successfully using a char buffer. total line "+ newlineCounter);
        }
    }

    /**
     * Checks if the current byte matches the specified pattern condition.
     *
     * @param ch      The current character being evaluated.
     * @param patternCondition The condition for this position in the pattern.
     *                         - `0` means the byte must be in the range '0'-'9' (48-57).
     *                         - Any other value means an exact byte match.
     * @return True if the byte satisfies the pattern condition; false otherwise.
     */
    private static boolean matchesPatternAt(char ch, int patternCondition) {
        if (patternCondition == 0) {
            // Condition `0` means the byte must be in the range '0'-'9' (48–57)
            return Character.isDigit(ch);
        } else {
            // Otherwise, the byte must match the exact `patternCondition` value
            return ch == patternCondition;
        }
    }
}
