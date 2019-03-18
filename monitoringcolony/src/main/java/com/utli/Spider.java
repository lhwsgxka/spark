package com.utli;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Properties;

public class Spider {

    public static void main(String[] args) throws InterruptedException {

        String filepath = "d:/123.txt";
        while (true) {
            String url_str = "http://192.168.192.3:50070/jmx?qry=java.lang:type=OperatingSystem";

            URL url = null;
            try {
                url = new URL(url_str);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            String charset = "utf-8";
            int sec_cont = 1000;
            try {
                URLConnection url_con = url.openConnection();
                url_con.setDoOutput(true);
                url_con.setReadTimeout(10 * sec_cont);
                url_con.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)");
                InputStream htm_in = url_con.getInputStream();
                //爬取数据
                String htm_str = InputStream2String(htm_in, charset);
             //   System.out.println(htm_str);
                JsonObject jsonObject = new JsonParser().parse(htm_str).getAsJsonObject();
                //获取指定数据
                String systemCpuLoad = jsonObject
                        .get("beans")
                        .getAsJsonArray()
                        .get(0)
                        .getAsJsonObject()
                        .get("SystemCpuLoad")
                        .getAsString();
                HashMap<String, String> map = new HashMap<String, String>();
                map.put("SystemCpuLoad", systemCpuLoad);
                String json = new Gson().toJson(map);
                //System.out.println(json);

                //将数据写入kafa
                String msg = json;
                Thread.sleep(3000);
                System.out.println(msg);
               kafka(msg);

                //保存数据
                // saveHtml(filepath,htm_str);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Method: saveHtml
     * Description: save String to file
     *
     * @param filepath file path which need to be saved
     * @param str      string saved
     */
    public static void saveHtml(String filepath, String str) {

        try {
      /*@SuppressWarnings("resource")
      FileWriter fw = new FileWriter(filepath);
      fw.write(str);
      fw.flush();*/
            OutputStreamWriter outs = new OutputStreamWriter(new FileOutputStream(filepath, true), "utf-8");
            outs.write(str);
            System.out.print(str);
            outs.close();
        } catch (IOException e) {
            System.out.println("Error at save html...");
            e.printStackTrace();
        }
    }

    /**
     * Method: InputStream2String
     * Description: make InputStream to String
     *
     * @param in_st   inputstream which need to be converted
     * @param charset encoder of value
     * @throws IOException if an error occurred
     */
    public static String InputStream2String(InputStream in_st, String charset) throws IOException {
        BufferedReader buff = new BufferedReader(new InputStreamReader(in_st, charset));
        StringBuffer res = new StringBuffer();
        String line = "";
        while ((line = buff.readLine()) != null) {
       /*     if (line.contains("SystemCpuLoad")) {
                res.append(line);
            }*/
            res.append(line);

        }
        return res.toString();
    }

    public static void kafka(String msg) {
        Properties properties = new Properties();
        //设置kafka集群的地址
        properties.put("bootstrap.servers", "localhost:9092");
        //ack模式，all是最慢但最安全的
        properties.put("acks", "all");
        //失败重试次数
        properties.put("retries", 0);
        //每个分区未发送消息总字节大小（单位：字节），超过设置的值就会提交数据到服务端
        properties.put("batch.size", 16384);
        //props.put("max.request.size",10);
        //消息在缓冲区保留的时间，超过设置的值就会被提交到服务端
        properties.put("linger.ms", 1);
        //整个Producer用到总内存的大小，如果缓冲区满了会提交数据到服务端
        //buffer.memory要大于batch.size，否则会报申请内存不足的错误
        properties.put("buffer.memory", 33554432);
        //序列化器
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        producer.send(new ProducerRecord<String, String>("666", msg));
        System.out.println("Sent:" + msg);
        producer.close();

    }

}