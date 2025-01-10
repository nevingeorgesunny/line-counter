package com.nevin.sunny;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ReplicaLogUtils {
    private static final String DATE_PATTERN = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{4}";
    private static final String LOG_PATTERN = "all_\\d{4}-\\d{2}-\\d{2}_\\d{2}\\.\\d{2}\\.\\d{2}\\.log";
    private static final List<LogFileAggregation> LOG_FILES_TO_AGGREGATE = List.of(
            new LogFileAggregation("jenkins.log", "nodes/master/logs/jenkins.log"),
            new LogFileAggregation("all_memory_buffer.log", "nodes/master/logs/all_memory_buffer.log"),
            new LogFileAggregation("all_rolled.log", "nodes/master/logs/all_rolled.log"));

    private record LogPattern(String pattern, String finalConcatenatedLogFileName) {}

    private record LogFileAggregation(String fileName, String destinationPath) {}


    private static char[][] buffers;
    private static int[] bufferIndices;
    private static int[] bufferLengths;

    public static void initializeBuffers(int numberOfReaders, int bufferSize) {
        buffers = new char[numberOfReaders][bufferSize];
        bufferIndices = new int[numberOfReaders];
        bufferLengths = new int[numberOfReaders];
    }

    public static StringBuffer readLineIntoStringBuffer(BufferedReader reader, int index, StringBuffer lineBuffer) throws IOException {
        if (lineBuffer == null) {
            lineBuffer = new StringBuffer();
        } else {
            lineBuffer.setLength(0); // Clear the existing content
        }

        char[] buffer = buffers[index];
        int bufferIndex = bufferIndices[index];
        int bufferLength = bufferLengths[index];

        while (true) {
            if (bufferIndex >= bufferLength) {
                bufferLength = reader.read(buffer);
                bufferIndex = 0;
                if (bufferLength == -1) {
                    break; // End of stream
                }
            }

            char c = buffer[bufferIndex++];
            if (c == '\n') {
                break; // Stop reading when newline is encountered
            }
            lineBuffer.append(c);
        }

        bufferIndices[index] = bufferIndex;
        bufferLengths[index] = bufferLength;

        // If the lineBuffer has no content, return null to indicate end of stream
        return lineBuffer.length() > 0 || bufferLength != -1 ? lineBuffer : null;
    }

    public static void processReplicaLogs(Path tempDir, Path logDir, List<String> exitedReplicas) throws IOException {
        List<LogPattern> logPatternToConcatenate =
                new ArrayList<>(List.of(new LogPattern(LOG_PATTERN, "all_rolled.log")));

        List<LogFileAggregation> logFilesToAggregate = new ArrayList<>(LOG_FILES_TO_AGGREGATE);


        for (LogPattern logPattern : logPatternToConcatenate) {
            concatenateFiles(tempDir,logDir, logPattern.pattern, logPattern.finalConcatenatedLogFileName,exitedReplicas);
        }

        for (LogFileAggregation logFileRecord : logFilesToAggregate) {

            //Testing with stringbuffer
            Path aggregatedLogPath = aggregateLogsStringBuffer(tempDir, logFileRecord.fileName);
            //Testing with string
            //TODO remove this
//            Path aggregedLogPath = aggregateLogs(tempDir, logFileRecord.fileName);
//            container.add(new FileContent(logFileRecord.destinationPath, aggregatedLogPath.toFile()));
        }
    }

    public static void concatenateFiles(Path tempDir,Path logDir, String logPattern, String outputFileName,List<String> exitedReplicas) throws IOException {
        boolean isLiveReplica = false;
        try (Stream<Path> replicaPath = Files.list(logDir)) {
            List<String> replicaFolders = replicaPath
                    .filter(Files::isDirectory)
                    .map(path -> {
                        Path fileName = path.getFileName();
                        return fileName != null ? fileName.toString() : null;
                    })
                    .filter(Objects::nonNull)
                    .toList();

            for (String replicaTempFolder : replicaFolders) {
                try (Stream<Path> paths = Files.walk(logDir.resolve(replicaTempFolder))) {
                    isLiveReplica = false;
                    List<Path> sortedLogFiles = paths.filter(Files::isRegularFile)
                            .filter(path -> {
                                Path fileName = path.getFileName();
                                return fileName != null && fileName.toString().matches(logPattern);
                            })
                            .sorted(Comparator.comparing(Path::getFileName))
                            .toList();
                    if (sortedLogFiles.isEmpty()) {
                        continue;
                    }
                    String replicaName = sortedLogFiles.get(0).getParent().getParent().getFileName().toString();
                    if(exitedReplicas.contains(replicaName)){
                        replicaName = replicaName + "_exited";
                    }

                    Path replicaFolder = tempDir.resolve(replicaName);
                    if (!Files.exists(replicaFolder)) {
                        Files.createDirectories(replicaFolder);
                    }

                    if (replicaFolder != null) {
                        Path outputFile = replicaFolder.resolve(outputFileName);

                        try (BufferedWriter writer = Files.newBufferedWriter(
                                outputFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                            for (Path inputFile : sortedLogFiles) {
                                try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
                                    reader.transferTo(writer);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static Path aggregateLogs(Path tempDir, String logFileName) throws IOException {
        // Fetch all the replica logs from the temp folder
        List<Path> logFiles = Files.walk(tempDir)
                .filter(p -> p.toString().endsWith(logFileName))
                .toList();

        List<BufferedReader> bufferedReaders = new ArrayList<>();
        try {
            for (Path path : logFiles) {
                bufferedReaders.add(Files.newBufferedReader(path));
            }

            Path outputFilePath = tempDir.resolve(logFileName);
            try (BufferedWriter writer =
                         Files.newBufferedWriter(outputFilePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {

                // Create an array to store the current line from each stream
                String[] currentLines = new String[bufferedReaders.size()];

                // Initialize the currentLines array with the first line from each stream
                for (int i = 0; i < bufferedReaders.size(); i++) {
                    currentLines[i] = bufferedReaders.get(i).readLine();
                }

                // Continue merging until all streams are exhausted
                boolean hasMoreData;
                String temp = null;
                do {
                    hasMoreData = false;
                    String minValue = null;
                    int minIndex = -1;
                    String replicaName = null;
                    // Find the stream with the smallest value
                    for (int i = 0; i < bufferedReaders.size(); i++) {
                        if (currentLines[i] != null) {
                            hasMoreData = true; // We still have data to process
                            if (minValue == null || currentLines[i].compareTo(minValue) < 0) {
                                replicaName = tempDir.relativize(logFiles.get(i))
                                        .getName(0)
                                        .toString();
                                minValue = currentLines[i];
                                minIndex = i;
                            }
                        }
                    }

                    minValue = "[" + replicaName + "] " + minValue;

                    // If there are still streams with data, write the smallest element
                    if (hasMoreData) {
                        // Move the pointer for the stream that had the smallest value
                        currentLines[minIndex] = bufferedReaders.get(minIndex).readLine();

                        // check if the next line start with not timestamp
                        if (!startsWithDate(currentLines[minIndex])) {
                            // if does not keep adding it to the previous line
                            StringBuilder minValueBuilder = new StringBuilder(minValue);
                            while (currentLines[minIndex] != null && !startsWithDate(currentLines[minIndex])) {
                                minValueBuilder.append("\n").append(currentLines[minIndex]);
                                currentLines[minIndex] =
                                        bufferedReaders.get(minIndex).readLine();
                            }
                            minValue = minValueBuilder.toString();
                        }

                        writer.write(minValue);
                        writer.newLine();
                    }

                } while (hasMoreData);
            }
            return outputFilePath;
        } finally {
            for (BufferedReader reader : bufferedReaders) {
                reader.close();
            }
        }
    }


    public static Path aggregateLogsStringBuffer(Path tempDir, String logFileName) throws IOException {
        // Fetch all the replica logs from the temp folder
        List<Path> logFiles = Files.walk(tempDir)
                .filter(p -> p.toString().endsWith(logFileName))
                .toList();

        BufferedReader[] bufferedReaders = new BufferedReader[logFiles.size()];

        try {
            String[] replicaName = new String[logFiles.size()];

            for (int i = 0; i < logFiles.size(); i++) {
                bufferedReaders[i] = Files.newBufferedReader(logFiles.get(i));
                replicaName[i] = "[" + tempDir.relativize(logFiles.get(i))
                        .getName(0) +  "] ";
            }

            Path outputFilePath = tempDir.resolve(logFileName);
            try (BufferedWriter writer =
                         Files.newBufferedWriter(outputFilePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {

                // Create an array to store the current line from each stream
                StringBuffer[] currentLines = new StringBuffer[bufferedReaders.length];


                initializeBuffers(bufferedReaders.length,8000);
                // Initialize the currentLines array with the first line from each stream
                for (int i = 0; i < bufferedReaders.length; i++) {
                    currentLines[i] = readLineIntoStringBuffer(bufferedReaders[i],i, currentLines[i]);
                }

                // Continue merging until all streams are exhausted
                boolean hasMoreData;
                do {
                    hasMoreData = false;
                    StringBuffer minValue = null;
                    if (minValue == null) {
                        minValue = new StringBuffer();
                    } else {
                        minValue.setLength(0); // Clear the existing content
                    }

                    int minIndex = -1;
                    // Find the stream with the smallest value
                    for (int i = 0; i < bufferedReaders.length; i++) {
                        if (currentLines[i] != null) {
                            hasMoreData = true; // We still have data to process
                            if (minValue.isEmpty() || currentLines[i].compareTo(minValue) < 0) {
                                minValue.append(currentLines[i]);
                                minIndex = i;
                            }
                        }
                    }

                    if(minIndex == -1 ){
                        //we have gone through all the logs
                        break;
                    }

                    minValue.insert(0,replicaName[minIndex]);

                    // If there are still streams with data, write the smallest element
                    if (hasMoreData) {
                        // Move the pointer for the stream that had the smallest value
                        currentLines[minIndex] = readLineIntoStringBuffer(bufferedReaders[minIndex],minIndex, currentLines[minIndex]);

                        // check if the next line start with not timestamp
                        if (!startsWithDate(currentLines[minIndex])) {
                            // if does not keep adding it to the previous line
                            while (currentLines[minIndex] != null && !startsWithDate(currentLines[minIndex])) {
                                minValue.append("\n").append(currentLines[minIndex]);
                                currentLines[minIndex] =
                                        readLineIntoStringBuffer(bufferedReaders[minIndex],minIndex, currentLines[minIndex]);
                            }
                        }

                        // Get the content of StringBuffer as a char array without creating a String object
                        char[] charArray = new char[minValue.length()];
                        minValue.getChars(0, minValue.length(), charArray, 0);

                        writer.write(charArray);
                        writer.newLine();
                    }

                } while (hasMoreData);
            }
            return outputFilePath;
        } finally {
            for (BufferedReader reader : bufferedReaders) {
                reader.close();
            }
        }
    }


    public static boolean startsWithDate(String input) {
        if (input == null || input.isEmpty()) {
            return false;
        }
        Pattern pattern = Pattern.compile(DATE_PATTERN);
        Matcher matcher = pattern.matcher(input);

        return matcher.find(); // Returns true if the date pattern is at the start
    }

    public static boolean startsWithDate(StringBuffer input) {
        if (input == null || input.isEmpty()) {
            return false;
        }
        Pattern pattern = Pattern.compile(DATE_PATTERN);
        Matcher matcher = pattern.matcher(input);

        return matcher.find(); // Returns true if the date pattern is at the start
    }


}
