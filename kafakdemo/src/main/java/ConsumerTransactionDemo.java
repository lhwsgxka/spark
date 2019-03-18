import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.sql.*;
import java.util.*;

/**
 * Author  : RandySun
 * Date    : 2017-08-13  17:06
 * Comment :
 */

public class ConsumerTransactionDemo {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //对于消费者消费的提交需要判断是否消费 利用事务提交的方式
        //还有就是对于消费接着上次的偏移量进行消费
        //利用数据库 第一次消费直接消费 第二次接着上次的偏移量消费
        //总的来讲 每次消费一点 提交一次是个事务
        //正确的是由于计算上涉及到数据的流动 为了保证数据和消费的同步 需要将数据的处理和
        //消费（偏移量）在数据库操作完成事务，以保证数据和消费的同步
        String consumerGroup = "f";
        String topic = "t2";
        int partition = 0;
        long offset = 0;
        //定义标志 是否是第一次消费
        boolean first = true;
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", consumerGroup);
        //自动提交
        properties.put("enable.auto.commit", "false");

        properties.put("auto.commit.interval.ms", "1000");

        properties.put("auto.offset.reset", "earliest");

        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);
        //查询数据库得到信息
        //连接数据库 获取偏移量
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123");
        connection.setAutoCommit(false);
        PreparedStatement select = connection.prepareStatement("select * from kafka_consumer where consumerGroup=?");
        select.setString(1, consumerGroup);
        ResultSet resultSet = select.executeQuery();
        //定义map  保存分区与偏移量信息 和主题
        Map<TopicPartition, Long> partitionOffset = new HashMap<TopicPartition, Long>();

        while (resultSet.next()) {
            //如果存在 修改状态
            first = false;
            topic = resultSet.getString(1);
            partition = resultSet.getInt(2);
            offset = resultSet.getLong(3);
            consumerGroup = resultSet.getString(4);
            TopicPartition tp = new TopicPartition(topic, partition);
            partitionOffset.put(tp, offset);
        }
        //如果是第一次  则 直接订阅topic
        if (first) {
            kafkaConsumer.subscribe(Arrays.asList(topic));
        } else {
            //修改数据
            Set<TopicPartition> topicPartitions = partitionOffset.keySet();
            //为消费者分配分区
            kafkaConsumer.assign(topicPartitions);
            //遍历每个分区  指定每个分区开始的地方
            for (TopicPartition tp :
                    topicPartitions) {
                //下次消费 应该从上次offset+1的地方开始
                kafkaConsumer.seek(tp, partitionOffset.get(tp) + 1);
            }
        }


        while (true) {
            try {
                Thread.sleep(5000);
                PreparedStatement update = connection.prepareStatement("insert into kafka_consumer  (topic,`partition`,`offset`,consumerGroup) values(?,?,?,?)  ON DUPLICATE KEY UPDATE `offset`=?");
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                    System.out.println();
                    System.out.printf("partition = %d, key = %s", record.partition(), record.key());
                    System.out.println();
                    System.out.printf("time = %d, topic = %s", record.timestamp(), record.topic());
                    System.out.println();
                    System.out.println("do something");
                    update.setString(1, topic);
                    update.setInt(2, record.partition());
                    update.setLong(3, record.offset());
                    update.setString(4, consumerGroup);
                    update.setLong(5, record.offset());
                    update.executeUpdate();
                    connection.commit();
                }

            } catch (Exception e) {
                System.out.println(e);
                connection.rollback();
            } finally {

            }
            // kafkaConsumer.commitSync();
        }

    }
}