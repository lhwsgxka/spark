import org.apache.spark.{SparkConf, SparkContext}

object AccDemo {
  def main(args: Array[String]): Unit = {
//累加器
    val sc=new SparkContext(new SparkConf().setMaster("local[*]").setAppName("as"))
    val accum = sc.longAccumulator("acc")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
    println(accum.value)
  }
}
