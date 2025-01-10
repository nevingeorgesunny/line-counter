package com.nevin.sunny;

import java.io.*;
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
    private static String byteProcessing = System.getProperty("byteProcessing");

    public static void main(String[] args ) throws IOException {
        long startTime = System.nanoTime();
        System.out.println("Starting the application...");

        if("true".equals(byteProcessing)){
            System.out.println("Using byte buffer");
            LineCounterByte.countLine();
        }else {
            System.out.println("Using char buffer");
            LineCounterChar.countLine();
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime; // in nanoseconds

        System.out.println("Execution time: " + duration / 1_000_000 + " ms");
    }
}
