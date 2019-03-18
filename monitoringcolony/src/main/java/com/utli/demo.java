package com.utli;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class demo {
    public static void main(String[] args) {
        String s="{\"SystemCpuLoad\":\"0.0\"}";
        JsonObject jsonObject = new JsonParser().parse(s).getAsJsonObject();

        String systemCpuLoad = jsonObject
             /*   .get("beans")
                .getAsJsonArray()
                .get(0)
                .getAsJsonObject()*/
                .get("SystemCpuLoad")
                .getAsString();
        System.out.println(systemCpuLoad);
    }
}
