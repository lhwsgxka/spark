import java.util.Properties

import com.zhiyou100.{Constants, PhoenixCURD}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

object MergeRecordAndReimburseByHospitals {

  case class Record(recordid: String, hospitalid: String, diseaseid: String,
                    departmentid: String, doctorid: String, flag: Int,
                    starttime: String, endtime: String, allcost: BigDecimal,
                    isrecovery: Int)

  case class Reimburse(recordid: String, reimbursetime: String, recost: BigDecimal)


  def main(args: Array[String]): Unit = {

    val recordTmpDir = "C:\\Users\\Administrator\\Desktop\\项目\\智能导诊数据工厂\\record\\ac28fcab-0c42-4a47-ba4a-9f25f71e2e4a.txt"
    val reimburseTmpDir = "C:\\Users\\Administrator\\Desktop\\项目\\智能导诊数据工厂\\reimburse\\adea1bcf-ad51-469c-822d-630e446c06c3.txt"
    import org.apache.spark.SparkContext._
    /*    //创建jobConf
        val conf = HBaseConfiguration.create()
        val jobConf = new JobConf(conf)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.set(TableOutputFormat.OUTPUT_TABLE,"test")*/

    val ss = SparkSession.builder().master(Constants.MASTER).appName(Constants.APP_NAME)
      .getOrCreate()

    import ss.implicits._

    val prop: Properties = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    prop.setProperty("driver", "org.apache.phoenix.jdbc.PhoenixDriver")

    val recordTmpDirDF = ss.sparkContext.textFile(recordTmpDir).map(
      line => ({
        val fields = line.split("\t")
        Record(fields(0), fields(1), fields(2), fields(3),
          fields(4), fields(5).toInt, fields(6), fields(7), BigDecimal(fields(8)), fields(9).toInt)
      })
    ).toDF()

    recordTmpDirDF.createOrReplaceTempView("record")

    val reimburseTmpDirDF = ss.sparkContext.textFile(reimburseTmpDir).map(
      line => ({
        val fields = line.split("\t")
        Reimburse(fields(0), fields(1), BigDecimal(fields(2)))
      })
    ).toDF()

    reimburseTmpDirDF.createOrReplaceTempView("reimburse")

    val tmp = ss.sql("SELECT \nl.recordid recordid,l.`hospitalid` hospitalid,l.`flag` flag ,l.`starttime` starttime,l.`endtime` endtime,l.`allcost` allcost,l.`isrecovery` isrecovery\n,r.recost recost\nFROM\nrecord l ,\n(SELECT recordid ,SUM(recost) recost FROM reimburse GROUP BY recordid ) r\nWHERE\nl.`recordid`=r.`recordid`")

    // tmp.show()

    tmp.createOrReplaceTempView("tmp")
    //新的
    val today = ss.sql("SELECT\nl.hospitalid hospitalid\n,\nl.hcount hcount,\nl.hcost hcost,\nl.hreimburse hreimburse,\nl.hrecovery hrecovery,\nl.hday hday,\nr.ocount ocount,\nr.ocost ocost,\nr.oreimburse oreimburse,\nr.orecovery orecovery,\nr.oday oday\nFROM\n(SELECT\nhospitalid, COUNT(hospitalid) hcount ,SUM(allcost) hcost ,SUM(recost) hreimburse ,SUM(isrecovery) hrecovery\n,SUM(DATEDIFF(endtime,starttime)+1) hday\nFROM tmp\nWHERE\nflag=1\nGROUP BY\nhospitalid) l,\n(SELECT\nhospitalid, COUNT(hospitalid) ocount ,SUM(allcost) ocost ,SUM(recost) oreimburse ,SUM(isrecovery) orecovery\n,SUM(DATEDIFF(endtime,starttime)+1) oday\nFROM tmp\nWHERE\nflag=2\nGROUP BY\nhospitalid) r\nWHERE\nl.hospitalid=r.hospitalid")

    today.createOrReplaceTempView("today")

    //today.show()

    //获取旧的相结合 旧的从hbase中读取 获取历史数据//spark sparksql

    //读取hbase的旧数据 创建旧表
    val history = ss.sqlContext.read.jdbc(Constants.DB_PHOENIX_URL, "GUIDE_HOSPITAL_HISTORY", prop)

    history.createOrReplaceTempView("history")

    //  history.show()

    //结合新旧数据完成新的数据

    // val accData= ss.sql("SELECT\nl.hospitalid hospitalid,\nl.hcount+(CASE r.hcount WHEN NULL THAN o ELSE r.hcount END) hcount,\nl.hcost+(CASE r.hcost WHEN NULL THAN o ELSE r.hcost END) hcost,\nl.hreimburse+(CASE r.hreimburse WHEN NULL THAN o ELSE r.hreimburse END) hreimburse,\nl.hrecovery+(CASE r.hrecovery WHEN NULL THAN o ELSE r.hrecovery END) hrecovery,\nl.hday+(CASE r.hday WHEN NULL THAN o ELSE r.hday END) hday,\nl.ocount+(CASE r.ocount WHEN NULL THAN o ELSE r.ocount END) ocount,\nl.ocost+(CASE r.ocost WHEN NULL THAN o ELSE r.ocost END) ocost,\nl.oreimburse+(CASE r.oreimburse WHEN NULL THAN o ELSE r.oreimburse END) oreimburse,\nl.orecovery+(CASE r.orecovery WHEN NULL THAN o ELSE r.orecovery END) orecovery,\nl.oday+ (CASE r.oday WHEN NULL THAN o ELSE r.oday END)\nFROM\nfresh l LEFT JOIN history r\nON\nl.hospitalid=r.hospitalid")

    val accData = ss.sql("select \nl.hospitalid hospitalid,\nl.hcount+r.hcount hcount,\nl.hcost+r.hcost hcost,\nl.hreimburse+r.hreimburse hreimburse,\nl.hrecovery+r.hrecovery hrecovery,\nl.hday+r.hday hday,\nl.ocount+r.ocount ocount,\nl.ocost+r.ocost ocost,\nl.oreimburse+r.oreimburse oreimburse,\nl.orecovery+r.orecovery orecovery,\nl.oday+r.oday oday\nfrom \ntoday l , history r\nwhere \nl.hospitalid=r.hospitalid\nunion\nselect \nl.hospitalid, l.hcount, l.hcost, l.hreimburse, l.hrecovery,l.hday,\nl.ocount,l.ocost,l.oreimburse,l.orecovery,l.oday\nfrom\ntoday l where hospitalid not in (select hospitalid from history)")
    //accData.show()
    /*   accData.write.format("jdbc")
         .option("url", "jdbc:mysql://localhost:3306/test")
         .option("dbtable", "accdata")
         .option("user", "root")
         .option("password", "123")
         .save()*/
    //保存最终数据
    accData.createOrReplaceTempView("accdata")

    //利用最终数据求指标数据
    //指标数据保存mysql
    val percentData = ss.sql("SELECT \nhospitalid ,\nhcount,\nhcost/hcount havgcost,\nhreimburse/hcount havgreimburse,\nhreimburse/hcost havgreproportion,\nhday/hcount havgday,\nhrecovery/hcount havgfinproportion,\nocount,\nocost/ocount oavgcost,\noreimburse/ocount oavgreimburse,\noreimburse/ocost oavgreproportion,\noday/ocount oavgday,\norecovery/ocount oavgfinproportion\nFROM\naccdata")
    //.show()
    percentData.write
      .mode("overwrite")
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "guide_hospital")
      .option("user", "root")
      .option("password", "123")
      .save()
    //总和数据写回hbase
    accData.write
      .format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("table", "guide_hospital_history")
      .option("zkUrl", "jdbc:phoenix:master:2181")
      .save()



    /*   val conf = HBaseConfiguration.create()
       //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
       conf.set("hbase.zookeeper.quorum","192.168.192.3")
       //设置zookeeper连接端口，默认2181
       conf.set("hbase.zookeeper.property.clientPort", "2181")
       val tablename = "record"
       //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
       val jobConf = new JobConf(conf)
       jobConf.setOutputFormat(classOf[TableOutputFormat])
       jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
   */
    //住院
    /* ss.sql("SELECT l.hid AS '医院名称' ,COUNT(l.rid) AS '总住院人数' ,SUM(l.`allcost`) '住院总费用', SUM(r.rec) '住院总报销' ,\nSUM(l.isrecovery) AS '住院总治愈人数' ,SUM(DATEDIFF(l.endtime,l.starttime)+1) AS '住院总天数'\nFROM\n(SELECT recordid rid ,allcost ,endtime ,starttime, isrecovery ,hospitalid hid FROM record WHERE flag=1 ) l ,\n(SELECT recordid rid ,SUM(recost) rec FROM reimburse GROUP BY recordid ) r \nWHERE l.rid=r.rid GROUP BY l.hid ;")
         .foreach(row=>{
           PhoenixCURD.insertRecord(row(0).toString,row(1).toString.toInt,BigDecimal.apply(row(2).toString).bigDecimal,BigDecimal.apply(row(3).toString).bigDecimal,row(4).toString.toInt,row(5).toString.toInt)
         })
     //提交并且关闭数据库链接
     PhoenixCURD.commitAndClose();*/

    /* ss.sql("SELECT l.hid ,COUNT(l.rid) " +
       " , SUM(l.`allcost`) , SUM(r.rec) ," +
       "SUM(l.isrecovery) , SUM(DATEDIFF(l.endtime,l.starttime)+1)" +
       " FROM (SELECT recordid rid ,allcost ,endtime ,starttime , " +
       "isrecovery ,hospitalid hid FROM record WHERE flag=1 ) l ," +
       "(SELECT recordid rid ,SUM(recost) rec FROM reimburse GROUP BY recordid ) r" +
       " WHERE l.rid=r.rid GROUP BY l.hid ").rdd.map(arr=>{
       /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
       val put = new Put(Bytes.toBytes(arr(0).toString))
       put.add(Bytes.toBytes("cf"),Bytes.toBytes("hcount"),Bytes.toBytes(arr(1).toString.toInt))
       put.add(Bytes.toBytes("cf"),Bytes.toBytes("hcost"),Bytes.toBytes(BigDecimal.apply(arr(2).toString).bigDecimal))
       put.add(Bytes.toBytes("cf"),Bytes.toBytes("hreimburse"),Bytes.toBytes(BigDecimal.apply(arr(3).toString).bigDecimal))
       put.add(Bytes.toBytes("cf"),Bytes.toBytes("hrecovery"),Bytes.toBytes(arr(4).toString.toInt))
       put.add(Bytes.toBytes("cf"),Bytes.toBytes("hday"),Bytes.toBytes(arr(5).toString.toInt))
       //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
       (new ImmutableBytesWritable, put)
     }).saveAsHadoopDataset(jobConf)*/


    //门诊
    /*
        ss.sql("SELECT l.hid AS '医院名称' ,COUNT(l.rid) AS '总门诊人数' ,SUM(l.`allcost`) '门诊总费用', SUM(r.rec) '门诊总报销' ,\nSUM(l.isrecovery) AS '门诊总治愈人数' ,SUM(DATEDIFF(l.endtime,l.starttime)+1) AS '门诊总天数'\nFROM\n(SELECT recordid rid ,allcost ,endtime ,starttime, isrecovery ,hospitalid hid FROM record WHERE flag=2 ) l ,\n(SELECT recordid rid ,SUM(recost) rec FROM reimburse GROUP BY recordid ) r \nWHERE l.rid=r.rid GROUP BY l.hid ;")
          .foreach(row=>{
            PhoenixCURD.insertRecord(row(0).toString,row(1).toString,BigDecimal.apply(row(2).toString).bigDecimal,currentTime)
          })
    */


  }
}
