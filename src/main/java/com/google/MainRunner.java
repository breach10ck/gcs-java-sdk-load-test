package com.google;

import com.google.cloud.storage.*;
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
    private static String PROJECT_ID = "projectId";
    private static String OBJECT_SIZE = "objectSize";
    private static String ENABLE_GRPC = "enableGrpc";

    private static Storage storage;

    public static void main(String[] args) throws InterruptedException {

        Options options = new Options();
        options.addOption(new Option(NUMBER_OF_THREADS, true,"Number of threads to spawn, default is 100."))
                .addOption(new Option(NUMBER_OF_OBJECTS, true,"Number of objects to create in each thread, default is 100."))
                .addOption(new Option(OBJECT_SIZE, true, "Bucket object size in bytes, default is 10000."))
                .addOption(new Option(ENABLE_GRPC, false, "Enable grpc direct path"))
                .addOption(Option.builder(BUCKET_NAME).hasArg().required(true).desc("GCS bucket name").build())
                .addOption(Option.builder(PROJECT_ID).hasArg().required(true).desc("Google cloud project id").build());
        CommandLine cmd;
        CommandLineParser parser = new DefaultParser();
        HelpFormatter helpFormatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
            boolean enableGrpc = cmd.hasOption(ENABLE_GRPC);
            System.out.printf("GRPC Protocol Enabled: %s\n", enableGrpc);
            LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
            StorageOptions.Builder storageBuilder = enableGrpc ? StorageOptions.grpc() : StorageOptions.newBuilder();
            storage = storageBuilder.setProjectId(cmd.getOptionValue(PROJECT_ID)).build().getService();
            ArrayList<WriteAndKeepReadingSampler> threads = new ArrayList<>();
            for(int i = 0; i < Integer.parseInt(cmd.getOptionValue(NUMBER_OF_THREADS, "100")); i++) {
                int numObjects = Integer.parseInt(cmd.getOptionValue(NUMBER_OF_OBJECTS, "100"));
                int objectSize = Integer.parseInt(cmd.getOptionValue(OBJECT_SIZE, "10000"));
                String bucketName = cmd.getOptionValue(BUCKET_NAME);
                threads.add(new WriteAndKeepReadingSampler(storage, bucketName, numObjects, objectSize));
                threads.get(threads.size()-1).start();
            }
            threads.get(0).join();

        } catch (ParseException e) {
            System.out.println(e.getMessage());
            helpFormatter.printHelp("Usage:", options);
        }
        System.exit(0);
    }
}
