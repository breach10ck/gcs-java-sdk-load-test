package com.google;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WriteAndKeepReadingSampler extends Thread {
    private static String FOLDER_NAME = "write_and_keep_reading/";
    private int numObjects;
    private int numBytes;
    private String bucketName;
    private Storage storage;

    WriteAndKeepReadingSampler(Storage storage, String bucketName,
                               int numObjects, int numBytes) {
        this.storage = storage;
        this.bucketName = bucketName;
        this.numObjects = numObjects;
        this.numBytes = numBytes;
    }

    public void run() {
        ImmutableList<String> objectNames = Util.generateObjectNames(numObjects);

        objectNames.forEach(objectName -> {
            writeObject(bucketName, FOLDER_NAME + objectName, numBytes);
        });
        System.out.printf("Thread %d wrote %d number of objects and is now reading objects...\n",
                Thread.currentThread().threadId(), objectNames.size());
        Random random = new Random();
        int count = 0;
        while(true) {
            int index = random.nextInt(objectNames.size());
            readObject(bucketName, FOLDER_NAME + objectNames.get(index));
            count++;
            if (count%100 == 0) {
                System.out.printf("Thread %d fetched 100 objects\n", Thread.currentThread().threadId());
            }
            count = count %100;
        }
    }

    private void readObject(String bucketName, String objectName) {
        try {
            try(ReadChannel readChannel = storage.reader(BlobId.of(bucketName, objectName))) {
                ByteBuffer bytes = ByteBuffer.allocate(64*1024);
                while (readChannel.read(bytes) > 0) {
                    bytes.flip();
                    bytes.clear();
                }
            }
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private void writeObject(String bucketName, String objectName, int numBytes) {
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        Storage.BlobWriteOption precondition;
        if (storage.get(bucketName, objectName) == null) {
            precondition = Storage.BlobWriteOption.doesNotExist();
        } else {
            precondition =
                    Storage.BlobWriteOption.generationMatch(
                            storage.get(bucketName, objectName).getGeneration());
        }
        try {
            WriteChannel writer = storage.writer(blobInfo, precondition);
            int bytesWritten = 0;
            while (bytesWritten < numBytes) {
                byte[] content = UUID.randomUUID().toString().getBytes();
                writer.write(ByteBuffer.wrap(content));
                bytesWritten += content.length;
            }
            writer.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (StorageException e) {
            System.out.println(e.getMessage());
        }
    }
}
