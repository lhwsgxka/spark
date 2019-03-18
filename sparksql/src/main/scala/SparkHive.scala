import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import spark.sql
    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM default.test").show()
  }

}
