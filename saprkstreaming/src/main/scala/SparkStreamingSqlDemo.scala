import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingSqlDemo {
  case class Word(word:String)
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc=new StreamingContext(conf,Seconds(5))
    //模拟从tcp端口读取数据
    val ds=ssc.socketTextStream("localhost",999)

    val sparkSession=SparkSession.builder().master("local[*]")
      .appName("streamingsql").getOrCreate()

    import sparkSession.implicits._
    ds.foreachRDD(
      rdd=>{
        val df=rdd.map(word=>Word(word)).toDF()
        df.createOrReplaceTempView("tmp")
        sparkSession.sql("select word,count(word) wordcount from tmp group by word order by  wordcount desc")
          .write.format("parquet").save("d:\\test\\sparkstramingsql"+"\\"+UUID.randomUUID().toString)
      }

    )

    //启动streaming context
    ssc.start()
    ssc.awaitTermination()




  }
}
