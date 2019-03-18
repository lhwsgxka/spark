import java.text.SimpleDateFormat

object SparkSqlUdfDemo {
  def main(args: Array[String]): Unit = {
    val date="2018-11-11 11:11:11"  //--->  "2018-11-11 11:11"

    val myudf=(date:String)=>{
        val format="yyyy-MM-dd HH:mm"
        val sdf=new SimpleDateFormat(format)
        sdf.format(sdf.parse(date))
    }

    //println(myudf(date))
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()
    import  spark.implicits._

    val df=spark.sparkContext.parallelize(List("2018-11-11 11:11:11")).toDF("mydate")
    df.createOrReplaceTempView("t")

    spark.sqlContext.udf.register("myudf",myudf)

    spark.sql("select myudf(mydate) from t").show()



  }
}
