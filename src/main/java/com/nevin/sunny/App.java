package com.nevin.sunny;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App
{
    public static final int BUFFER_SIZE = 8000;
    // Define the input and output file paths
    static String inputFile = "/Users/nsunny/dev/exp/log-aggregation/out.txt";
    static String outputFile = "/Users/nsunny/dev/exp/log-aggregation/out2.txt";


    public static void main( String[] args ) throws IOException {

        long startTime = System.nanoTime();

        System.out.println("Starting the application...");

//        charbufferTrasfer();
//        lazyLoad();

        ReplicaLogUtils.processReplicaLogs(Path.of("/Users/nsunny/dev/exp/log-aggregation/replic-logs/temp")
                ,Path.of("/Users/nsunny/dev/exp/log-aggregation/replic-logs")
                , List.of("mc-66d9cb4bbc-11111","mc-66d9cb4bbc-222222","mc-66d9cb4bbc-111112","mc-66d9cb4bbc-2222223"));

        long endTime = System.nanoTime();
        long duration = endTime - startTime; // in nanoseconds

        System.out.println("Execution time: " + duration / 1_000_000 + " ms");

    }

    static void lazyLoad(){
        Path sourcePath = Path.of(inputFile);
        Path targetPath = Path.of(outputFile);

        try (
                // Open FileChannels for source (read) and target (write)
                FileChannel sourceChannel = FileChannel.open(sourcePath, StandardOpenOption.READ);
                FileChannel targetChannel = FileChannel.open(targetPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        ) {
            // Allocate a ByteBuffer of the specified size
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

            // Loop through the source file, reading and writing chunks
            while (sourceChannel.read(buffer) != -1) {
                buffer.flip(); // Prepare buffer for reading
                while (buffer.hasRemaining()) {
                    targetChannel.write(buffer); // Write buffer data to the target file
                }
                buffer.clear(); // Clear buffer for the next read
            }

            System.out.println("File copy completed successfully.");

        } catch (IOException e) {
            System.err.println("Error during file operation: " + e.getMessage());
            e.printStackTrace();
        }

    }


    static void charbufferTrasfer(){
        // Define a char buffer size (e.g., 8 KB)
        char[] buffer = new char[BUFFER_SIZE];

        try (
                FileReader fr = new FileReader(inputFile);
                FileWriter fw = new FileWriter(outputFile)
        ) {
            int charsRead;
            while ((charsRead = fr.read(buffer)) != -1) {
                fw.write(buffer, 0, charsRead); // Write only the characters read in this iteration
            }
            System.out.println("File streamed successfully using a char buffer.");
        } catch (IOException e) {
            System.err.println("An error occurred while processing the file: " + e.getMessage());
        }

    }
}
