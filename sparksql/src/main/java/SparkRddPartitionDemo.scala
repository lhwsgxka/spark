import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object SparkRddPartitionDemo {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("partition"))

    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 22345678))

    val rdd2=sc.textFile("d:\\test\\123.txt")
        .flatMap(line=>line.split(" ")).map(word=>(word,1))

    val rdd3=rdd2.reduceByKey(_+_)

    println(rdd3.getNumPartitions)

    println(rdd3.partitioner.get)

    //
    val rdd4=rdd3.partitionBy(new Mypartitioner(2))

    println(rdd4.partitioner.get)

    rdd4.mapPartitionsWithIndex((index,p)=>{
      p.foreach(e=>println("p:"+index+"e:"+e))
      p
  })
  }.collect()
}
//自定义分区

class Mypartitioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = if (key.toString.startsWith("h")) 0 else 1
}

