import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("d:\\1.txt")

    rdd.flatMap(line => line.split(" "))
     .groupBy(word => word)
     .map(kv => (kv._1, kv._2.size))
    //  .collect()
      .foreach(println)

    Thread.sleep(10000000)

  }
}
