import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;


/**
 * Author  : lhw
 * Date    : 2017-08-13  16:23
 * Comment :
 */
public class ProducerDemo {

    public static void main(String[] args) throws IOException {
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
        Producer<String, String> producer = null;
        while (true) {
            try {
                InputStreamReader isr = new InputStreamReader(System.in);
                BufferedReader br = new BufferedReader(isr);
                String msg;
                msg = br.readLine();
                producer = new KafkaProducer<String, String>(properties);
                producer.send(new ProducerRecord<String, String>("777", msg));
                System.out.println("Sent:" + msg);
            } catch (Exception e) {
                e.printStackTrace();

            } finally {
                producer.close();
            }
        }


    }
}