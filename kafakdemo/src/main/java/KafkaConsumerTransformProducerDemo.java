import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
*@ClassName:KafkaConsumerTransformProducerDemo
 @Description:TODO
 @Author:
 @Date:2018/11/20 15:13
 @Version:v1.0
*/
/*
    消费来自kafka的数据  最后写会kafka
 */
public class KafkaConsumerTransformProducerDemo {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = getProducer();
        KafkaConsumer<String, String> consumer = getConsumer();
        //开启事务
        producer.initTransactions();
        while (true) {
            try {
                producer.beginTransaction();
                ConsumerRecords<String, String> consumerRecords = consumer.poll(500);
                Map<TopicPartition, OffsetAndMetadata> hashMap = new HashMap();
                for (ConsumerRecord record :
                        consumerRecords) {
                    System.out.println(record.value());
                    if (record.value().toString().startsWith("w")) {

                        producer.send(new ProducerRecord<String, String>("t2", (String) record.value()));
                        // producer.flush();
                        hashMap.put(new TopicPartition("lhw", record.partition()), new OffsetAndMetadata(record.offset()));
                    }

                }
                producer.sendOffsetsToTransaction(hashMap, "12344");
                producer.commitTransaction();
            }catch (Exception e){
                //回滚
                producer.abortTransaction();

            }
        }

    }

    private static KafkaConsumer<String, String> getConsumer() {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");

        properties.put("group.id", "12344");

        properties.put("enable.auto.commit", "false");

        properties.put("auto.commit.interval.ms", "1000");

        properties.put("auto.offset.reset", "earliest");//

        properties.put("session.timeout.ms", "30000");

        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        kafkaConsumer.subscribe(Arrays.asList("lhw"));
        return kafkaConsumer;

    }

    private static KafkaProducer<String, String> getProducer() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("transactional.id","oiecw");
        properties.put("acks","all");
        properties.put("enable.idempotence", true);
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 10);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(properties);
    }
}
