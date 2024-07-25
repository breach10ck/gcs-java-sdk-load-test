package com.google;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;

import java.io.ByteArrayInputStream;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class WriteAndKeepReadingSampler extends Thread {
    private static String FOLDER_NAME = "write_and_keep_reading/";
    private int numObjects;
    private String bucketName;
    private int objectSize;
    private Storage storage;

    WriteAndKeepReadingSampler(Storage storage, String bucketName,
                               int numObjects, int objectSize) {
        this.storage = storage;
        this.bucketName = bucketName;
        this.numObjects = numObjects;
        this.objectSize = objectSize;
    }

    public void run() {
        ImmutableList<String> objectNames = Util.generateObjectNames(numObjects);

        objectNames.forEach(objectName -> {
            writeObject(bucketName, FOLDER_NAME + objectName);
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
            byte[] content = storage.readAllBytes(bucketName, objectName);
            //System.out.printf("Fetched object %s of size %d\n", objectName, content.length);
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
        }
    }

    private void writeObject(String bucketName, String objectName) {
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        // Optional: set a generation-match precondition to avoid potential race
        // conditions and data corruptions. The request returns a 412 error if the
        // preconditions are not met.
        Storage.BlobWriteOption precondition;
        if (storage.get(bucketName, objectName) == null) {
            // For a target object that does not yet exist, set the DoesNotExist precondition.
            // This will cause the request to fail if the object is created before the request runs.
            precondition = Storage.BlobWriteOption.doesNotExist();
        } else {
            // If the destination already exists in your bucket, instead set a generation-match
            // precondition. This will cause the request to fail if the existing object's generation
            // changes before the request runs.
            precondition =
                    Storage.BlobWriteOption.generationMatch(
                            storage.get(bucketName, objectName).getGeneration());
        }
        ByteArrayInputStream inputStream = new ByteArrayInputStream(Util.generateObjectContent(objectSize).getBytes());
        storage.create(blobInfo, inputStream, precondition);
    }

}
