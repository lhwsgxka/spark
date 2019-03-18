object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val ss = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import ss.implicits._

    val df=ss.read
      .json("D:\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")


    df.show()
  //  df.createOrReplaceTempView("p")

 //   spark.sql("select name from p where age > 18").show()

    //df.show()




  }

}

