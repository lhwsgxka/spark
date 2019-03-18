import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/*
  优化了数据库连接池
 */
object demo {

  def getfromOffsets(group: String) = {
    val conn = MysqlConnectionPool.getConnection()
    val select = conn.prepareStatement("select * from kafka_consumer where consumer_group=?")
    select.setString(1, group)
    val resultSet = select.executeQuery
    val partitionOffset: util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]

    while (resultSet.next()) { //数据库有数据 说明不是第一次消费  first 置为false
      //从数据库获得  1  topic 2 分区  3 偏移量
      val tp: TopicPartition = new TopicPartition(
        resultSet.getString(1), resultSet.getInt(2))
      partitionOffset.put(tp, resultSet.getLong(3))
    }
    resultSet.close()
    conn.close()
    import scala.collection.JavaConverters._
    partitionOffset.asScala
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    val group = "ssc3";
    val topic = "t1"
    //获取保存在数据库的偏移量
    val fromOffsets = getfromOffsets(group)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(topic)
    //第一次
    var stream: DStream[ConsumerRecord[String, String]] = null
    if (fromOffsets.size == 0) {
      //第一次的时候执行原来的
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
    } else {
      //当不是第一次，将数据库的数据配置进去
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
      )
    }
    //执行业务逻辑
    stream //.map(record => (record.key, record.value))
      .foreachRDD(rdd => {
      //获取偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        //设置更新偏移量到数据库
        val conn = MysqlConnectionPool.getConnection()
        conn.setAutoCommit(false)
        val update = conn.prepareStatement("insert into kafka_consumer  (topic,`partition`,`offset`,consumer_group) values(?,?,?,?)  ON DUPLICATE KEY UPDATE `offset`=?")
        try {
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          //  println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          //业务逻辑


          //保存偏移量
          update.setString(1, o.topic)
          update.setInt(2, o.partition)
          update.setLong(3, o.untilOffset)
          update.setString(4, group)
          update.setLong(5, o.untilOffset)
          update.executeUpdate
          conn.commit()
        } catch {
          case e: Exception => conn.rollback()
        } finally {
          MysqlConnectionPool.close(conn)
        }
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
