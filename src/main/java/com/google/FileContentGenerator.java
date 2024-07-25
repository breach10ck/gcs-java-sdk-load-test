package com.google;


import com.google.common.collect.ImmutableList;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutionException;



public class FileContentGenerator extends Thread {
    private static int PARALLELISM = 10;
    private int numFiles;
    private int numBytes;
    private String folderPath;

    FileContentGenerator(int numFiles, int numBytes, String folderPath) {
        this.numFiles = numFiles;
        this.numBytes = numBytes;
        this.folderPath = folderPath;
    }

    public void generate() throws InterruptedException {
        if (numFiles < 10) {
            this.start();
            this.join();
            return;
        }
        ArrayList<FileContentGenerator> threads = new ArrayList<>();
        for (int i =0; i < PARALLELISM; i++) {
            int filesPerThread = (numFiles/PARALLELISM) + (i <= numFiles%PARALLELISM ? 1 : 0);
            threads.add(new FileContentGenerator(filesPerThread, numBytes, folderPath));
            threads.get(threads.size()-1).start();
        }
        threads.forEach(fileContentGenerator -> {
            try {
                fileContentGenerator.join();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        });
    }

    public void run() {
        ImmutableList<String> fileNames = Util.generateObjectNames(numFiles);
        fileNames.forEach(fileName -> {
            try {
                String filePath = folderPath + "/" + fileName;
                File file = new File(filePath);
                file.getParentFile().mkdir();
                BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
                int totalBytesWritten = 0;
                while (totalBytesWritten < numBytes) {
                    String chunk = UUID.randomUUID().toString();
                    writer.write(chunk);
                    totalBytesWritten += chunk.length();
                }
                writer.close();
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        });
        System.out.printf("Written %d files with content of size %d on disk\n", numFiles, numBytes);
    }
}
