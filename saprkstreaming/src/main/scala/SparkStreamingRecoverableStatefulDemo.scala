import SparkStreamingRecoverableStatefulDemo.functionToCreateContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingRecoverableStatefulDemo {
  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate("d:\\test\\statestream", functionToCreateContext _)

    //启动streaming context
    ssc.start()
    ssc.awaitTermination()


  }
  def functionToCreateContext(): StreamingContext = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc=new StreamingContext(conf,Seconds(5))
    //有状态的计算  需要还原点 用来保存状态
   // ssc.checkpoint("d:\\test\\statestream")
    //模拟从tcp端口读取数据
    val ds=ssc.socketTextStream("localhost",999)

    def update(current:Seq[Int],old:Option[Int])={
      val newValue=current.sum
      //因为第一次key出现的时候  是没有状态的  需要初始化状态
      val oldValue=old.getOrElse(0)
      Some(newValue+oldValue)
    }

    //world 8
    //world world
    //seq[1,1]
    ds.map(word=>(word,1))
      .updateStateByKey(update _)
      .print()

    ssc
  }
}
