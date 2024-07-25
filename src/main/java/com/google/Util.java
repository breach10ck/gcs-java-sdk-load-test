package com.google;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.UUID;

public class Util {

    public static ImmutableList<String> generateObjectNames(int objectCount) {
        ArrayList<String> objectNames = new ArrayList<>();
        for(int i = 0; i < objectCount; i++) {
            objectNames.add(UUID.randomUUID().toString());
        }
        return ImmutableList.copyOf(objectNames);
    }

    public static String generateObjectContent(int bytes) {
        String randomStr = UUID.randomUUID().toString();
        while(randomStr.length() < bytes) {
            randomStr += UUID.randomUUID().toString();
        }
        return randomStr;
    }
}
