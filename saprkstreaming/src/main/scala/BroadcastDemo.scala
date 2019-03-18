import org.apache.spark.{SparkConf, SparkContext}

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("as"))
    //在map端做两表关联
    val broadcastVar = sc.broadcast(List("1 human", "2 animal"))
    sc.parallelize(List("1 jack 1", "2 rose 1", "3 tom 2", "4 jerry 2"))
      .map(line => {
        val arr = line.split(" ")

        val cid = arr(2)
        //broadcastVar.value  定义的全部域
        val className = broadcastVar.value.map(line => line.split(" "))
          .filter(arr => arr(0).equals(cid))
          .map(arr => arr(1)).head
        //返回值
        (arr(0), arr(1), className)
      }).foreach(println)
    //stu
    //1 jack 1
    //2 rose 1
    //3 tom 2
    //4 jerry 2

    // class
    //1 human
    //2 animal
  }
}
