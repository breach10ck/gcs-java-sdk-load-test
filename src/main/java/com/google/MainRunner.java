package com.google;

import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableList;
import io.grpc.grpclb.GrpclbLoadBalancerProvider;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Option;
import io.grpc.LoadBalancerRegistry;

import java.util.ArrayList;

public class MainRunner {
    private static String BUCKET_NAME = "bucketName";
    private static String NUMBER_OF_THREADS = "numThreads";
    private static String NUMBER_OF_OBJECTS = "numObjects";
    private static String NUMBER_OF_BUCKETS = "numBuckets";
    private static String PROJECT_ID = "projectId";
    private static String OBJECT_SIZE = "objectSize";
    private static String ENABLE_GRPC = "enableGrpc";
    private static String FOLDER_PATH = "fileContentFolder";

    public static void main(String[] args) throws InterruptedException {

        Options options = new Options();
        options.addOption(new Option(NUMBER_OF_THREADS, true,"Number of threads to spawn per bucket, default is 10."))
                .addOption(new Option(NUMBER_OF_OBJECTS, true,"Number of objects to create in each thread, default is 10."))
                .addOption(new Option(NUMBER_OF_BUCKETS, true,"Number of buckets to create, default is 1."))
                .addOption(new Option(OBJECT_SIZE, true, "Bucket object size in bytes, default is 10000."))
                .addOption(new Option(FOLDER_PATH, true, "Folder path where contents will be generated and stored."))
                .addOption(new Option(ENABLE_GRPC, false, "Enable grpc direct path"))
                .addOption(Option.builder(PROJECT_ID).hasArg().required(true).desc("Google cloud project id").build());
        CommandLine cmd;
        CommandLineParser parser = new DefaultParser();
        HelpFormatter helpFormatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
            boolean enableGrpc = cmd.hasOption(ENABLE_GRPC);
            System.out.printf("GRPC Protocol Enabled: %s\n", enableGrpc);
            LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());

            String projectId = cmd.getOptionValue(PROJECT_ID);
            int numThreads = Integer.parseInt(cmd.getOptionValue(NUMBER_OF_THREADS, "10"));
            int numObjects = Integer.parseInt(cmd.getOptionValue(NUMBER_OF_OBJECTS, "10"));
            int numBuckets = Integer.parseInt(cmd.getOptionValue(NUMBER_OF_BUCKETS, "1"));
            int objectSize = Integer.parseInt(cmd.getOptionValue(OBJECT_SIZE, "10000"));
            String folderPath = cmd.getOptionValue(FOLDER_PATH, "generated_contents");

            new FileContentGenerator(numObjects*numThreads/10, objectSize, folderPath).generate();

            ImmutableList<String> buckets = createBuckets(projectId, numBuckets);
            ArrayList<WriteAndKeepReadingSampler> threads = new ArrayList<>();
            buckets.forEach(bucketName -> {
                Storage storage = createStorageObject(projectId, enableGrpc);
                for(int i = 0; i < numThreads; i++) {
                    threads.add(new WriteAndKeepReadingSampler(storage, bucketName, numObjects, folderPath));
                    threads.get(threads.size()-1).start();
                }
            });

            threads.get(0).join();

        } catch (ParseException e) {
            System.out.println(e.getMessage());
            helpFormatter.printHelp("Usage:", options);
        }
        System.exit(0);
    }

    private static Storage createStorageObject(String projectId, boolean enableGrpc) {
        StorageOptions.Builder storageBuilder = enableGrpc ? StorageOptions.grpc() : StorageOptions.newBuilder();
        return storageBuilder.setProjectId(projectId).build().getService();
    }

    private static ImmutableList<String> createBuckets(String projectId, int numBuckets) {
        ArrayList<String> buckets = new ArrayList<>();
        Storage storage = createStorageObject(projectId, false);
        for(int i=0; i<numBuckets; i++) {
            String bucketName = "gcloud-storage-grpc-test-stp-ajayky-java-"+String.valueOf(i);
            if(storage.get(bucketName) == null) {
                storage.create(BucketInfo.of(bucketName));
            }
            System.out.printf("Bucket %s created.%n", bucketName);
            buckets.add(bucketName);
        }
        return ImmutableList.copyOf(buckets);
    }

}
