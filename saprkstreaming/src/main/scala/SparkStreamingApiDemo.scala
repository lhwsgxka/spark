import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._ // not necessary since Spark 1.3
object SparkStreamingApiDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    //模拟从tcp端口读取数据
    val ds = ssc.socketTextStream("localhost", 999)
    ds.foreachRDD(
      rdd => rdd.map(word => (word, 1))
      .reduceByKey(_ + _).collect().foreach(println))

    ds.transform(
      rdd => rdd.map
      (word => (word, 1)).reduceByKey(_ + _)).print()

    ds.persist()


    //ds.persist(StorageLevel.DISK_ONLY)

    //启动streaming context
    ssc.start()
    ssc.awaitTermination()


  }
}
