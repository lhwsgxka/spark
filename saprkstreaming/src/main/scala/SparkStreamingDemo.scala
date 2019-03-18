import org.apache.spark._
import org.apache.spark.streaming._

object SparkStreamingDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")

    val ssc = new StreamingContext(conf, Seconds(5))

    val ds = ssc.socketTextStream("localhost", 999)

    ds.map(word => (word, 1)).reduceByKey(_ + _).print()

    ssc.start()

    ssc.awaitTermination()


  }
}
