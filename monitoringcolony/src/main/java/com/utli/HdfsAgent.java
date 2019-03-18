/*
package com.utli;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.SystemDefaultHttpClient;

import java.io.IOException;
import java.io.InputStream;

public class HdfsAgent {
    public static void main(String[] args) throws IOException {
        String url = "http://192.168.184.6:50070/jmx?qry=java.lang:type=OperatingSystem";
        CloseableHttpResponse response = new SystemDefaultHttpClient()
                .execute(new HttpGet(url));
        InputStream inputStream = response.getEntity().getContent();
        String json = IOUtils.toString(inputStream);
        Gson gson = new Gson();
        JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
        String systemCpuLoad = jsonObject
                .get("beans")
                .getAsJsonArray()
                .get(0)
                .getAsJsonObject()
                .get("SystemCpuLoad")
                .getAsString();

        JsonParser parser = new JsonParser();
        JsonArray jsonArray = parser.parse(json).getAsJsonArray();


        //String systemCpuLoad = jsonArray.get(0).getAsJsonObject().get("SystemCpuLoad").getAsString();
        System.out.println(systemCpuLoad);

    }
}
*/
