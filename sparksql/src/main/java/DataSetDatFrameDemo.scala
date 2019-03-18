import org.apache.spark.sql.SparkSession

object DataSetDatFrameDemo {
  case class CityIp(city:String,ip:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    val rdd=spark.sparkContext
      .textFile("d:\\test\\city-ip.txt").map(line=>line.split(" "))
    .map(arr=>CityIp(arr(0),arr(1)));
    ///rdd.collect().foreach(println)
    import spark.implicits._

    val df=rdd.toDF()

    val ds=rdd.toDS()

    df.filter(row=>row.getAs[String]("city").equals("guangzhou")).show()

    df.createOrReplaceTempView("cip1")

    ds.filter(cityIp=>cityIp.city.equals("guangzhou")).show()

    ds.createOrReplaceTempView("cip2")

    spark.sql("select * from cip1 where city='guangzhou'").show()

    spark.sql("select * from cip2 where city='guangzhou'").show()


  }
}
