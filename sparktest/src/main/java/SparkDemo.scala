import org.apache.spark.launcher.SparkLauncher

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val handle = new SparkLauncher()
      //jar包路径\\Users\\root\\Desktop\\spark-demo\\sparkcore\\target\\spark-core-1.0-SNAPSHOT.jar
      .setAppResource("D:\\project\\sparkdemo\\sparktest\\target\\sparktest-1.0-SNAPSHOT.jar")
      //运行主类
      .setMainClass("SparkYarnDemo")
      //yarn运行
      .setMaster("yarn")
      //部署模式client或者cluster
      .setDeployMode("cluster")
      .setAppName("de mo")
      //设置driver内存 可以不设置
        .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
      //设置sparkhome如果配置了环境变量 可以不写
      .setSparkHome("D:\\spark-2.3.0-bin-hadoop2.7")
      //打印详情 可以不写
      .setVerbose(true)
      //启动 应用
      .startApplication();
    //阻塞等待任务完成
    while (!handle.getState.isFinal) {
      Thread.sleep(2000)
      println(handle.getState)

    }
  }
}
