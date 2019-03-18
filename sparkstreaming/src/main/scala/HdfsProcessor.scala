import com.google.gson.Gson
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HdfsProcessor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    val group = "3";

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("666")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //
    val windowedStrem = stream.map(record => (record.key(), record.value()))
      .map(valuse => {
        val jsonstr = valuse._2
        //反序列化
        new Gson().fromJson(jsonstr, classOf[Message])
      }).window(Seconds(15), Seconds(5))
    //过滤
    windowedStrem.filter(message => "hdfs".equals(message.getCompType)
      && "namenode".equals(message.getConfigType)
      && "cpuload".equals(message.getMetricCode))
      //转成kv
      .map(message => (message.getHostIp, message.getMetricValue))
      .groupByKey()
      //过滤出来负载大的节点
      .filter(kv => kv._2.filter(value => value.toDouble > 0.0).size >= 5)
      .foreachRDD(
        rdd => rdd.foreach(
          kv => {
            //打印
            val ip = kv._1
            print("ip:" + ip + "cpu load too high,please attention")
          }
        )
      )

    ssc.start()
    ssc.awaitTermination()

  }

}
