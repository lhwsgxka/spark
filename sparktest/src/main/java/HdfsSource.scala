import org.apache.spark.{SparkConf, SparkContext}

object HdfsSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
   // conf.setMaster("local[*]").setAppName("hdfs")
    val sc = new SparkContext(conf)

   val rdd = sc.textFile("hdfs://192.168.192.3:9000/readme.txt")
 //  val rdd = sc.textFile("d:\\1.txt")
    rdd.flatMap(line => line.split(" ")).map(word => (word, 1)).
      reduceByKey(_ + _)
     .saveAsTextFile("hdfs://192.168.192.3:9000/out1")

    // .collect().foreach(println)

  }

}
