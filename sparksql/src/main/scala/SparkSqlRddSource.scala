object SparkSqlRddSource {

  case class Word(word:String)

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    val rdd=spark.sparkContext
      .textFile("d:\\test\\123.txt").flatMap(line=>line.split(" "))
    ///rdd.collect().foreach(println)
    import spark.implicits._
    //加模式两种方法
    //直接指定列名  toDf
    val df=rdd.toDF("word")
    //通过模式类 todf
    val df2=rdd.map(word=>Word(word)).toDF()
    // df.show()
    /// df2.show()
    df.createOrReplaceTempView("wc")

    spark.sql("select word,count(word) wordcount from wc group by word")
      .write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "wc")
      .option("user", "root")
      .option("password", "123456")
      .save()


  }

}
