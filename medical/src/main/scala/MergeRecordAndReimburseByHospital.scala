import com.zhiyou100.Constants
import org.apache.spark.sql.SparkSession

object MergeRecordAndReimburseByHospital {

  //recordid hospitalid
  // diseaseid departmentid doctorid
  // flag starttime endtime allcost isrecovery
  case class Record(recordid: String, hospitalid: String, diseaseid: String,
                    departmentid: String, doctorid: String, flag: Int,
                    starttime: String, endtime: String, allcost: BigDecimal,
                    isrecovery: Int)

  //recordid reimbursetime recost
  case class Reimburse(recordid: String, reimbursetime: String, recost: BigDecimal)

  def main(args: Array[String]): Unit = {
    /* if (args.length == 0) {
       throw new IllegalArgumentException("请输入路径")
     }
     val recordTmpDir = args(0)
     val reimburseTmpDir = args(1)*/
    val recordTmpDir = "C:\\Users\\Administrator\\Desktop\\项目\\智能导诊数据工厂\\record\\ac28fcab-0c42-4a47-ba4a-9f25f71e2e4a.txt"
    val reimburseTmpDir = "C:\\Users\\Administrator\\Desktop\\项目\\智能导诊数据工厂\\reimburse\\adea1bcf-ad51-469c-822d-630e446c06c3.txt"
    val ss = SparkSession.builder().master(Constants.MASTER).appName(Constants.APP_NAME)
      .getOrCreate()
    import ss.implicits._
    //recordid hospitalid
    // diseaseid departmentid doctorid
    // flag starttime endtime allcost isrecovery
    val recordTmpDirDF = ss.sparkContext.textFile(recordTmpDir).map(
      line => ({
        val fields = line.split("\t")
        Record(fields(0), fields(1), fields(2), fields(3),
          fields(4), fields(5).toInt, fields(6), fields(7), fields(8).toDouble, fields(9).toInt)
      })
    ).toDF()

    recordTmpDirDF.createOrReplaceTempView("record")

    val reimburseTmpDirDF = ss.sparkContext.textFile(reimburseTmpDir).map(
      line => ({
        val fields = line.split("\t")
        Reimburse(fields(0), fields(1), fields(2).toDouble)
      })
    ).toDF()
    /*   reimburseTmpDirDF.createOrReplaceTempView("reimburse")
       ss.sql("select * from record").show()
       reimburseTmpDirDF.write
         .format("jdbc")
         .option("url", "jdbc:mysql://localhost:3306/test")
         .option("dbtable", "reimburse")
         .option("user", "root")
         .option("password", "123")
         .save()*/
    //  ss.sql("select * from reimburse").show()

    //进行数据的查询
    //   ss.sql("select * from record r,reimburse l where r.recordid=l.recordid").show()
    //1总就诊人数=总就诊人数

    //ss.sql("select `count`(recordid) from record ").show()

    //人均报销费用=总报销/总就诊人数
    //ss.sql("select AVG(recost) from reimburse ").show()

    //人均报销比例=总报销/总花费
    //   ss.sql("select sum(r.s1)/sum(l.allcost) from (select recordid rid,sum(recost) s1 from reimburse group by recordid) r,record l where r.rid=l.recordid").show()

    //人均住院天数=总住院天数/总就诊人数
    //ss.sql("select sum(datediff(endtime,starttime)+1)/count(recordid) from record ").show()

    //治愈率=总治愈人数/总就诊人数
    //record
    // ss.sql("select count(l.rid)/count(r.recordid) from record r left join (select recordid rid from record where isrecovery=1) l on l.rid=r.recordid").show()
    //ss.sql("select count(recordid)/(select count(recordid) from record ) from record where isrecovery=1").show()

    //住院总就诊人数=住院总就诊人数
    //ss.sql("select count(recordid) from record where flag=1 ").show()

    //住院人均花费=住院总花费/住院总就诊人数
    //ss.sql("select  sum(allcost)/count(recordid) from record where flag=1").show()


    /*  case class Record(recordid: String, hospitalid: String, diseaseid: String,
                        departmentid: String, doctorid: String, flag: Int,
                        starttime: String, endtime: String, allcost: BigDecimal,
                        isrecovery: Int)*/
    //record reimburse
    //  case class Reimburse(recordid: String, reimbursetime: String, recost: BigDecimal)
    //住院人均报销费用=住院总报销/住院总就诊人数
    /* ss.sql("select sum(r.rec)/count(r.rid) from " +
       "(select recordid rid from record  where flag=1) l ," +
       "(select recordid rid ,sum(recost) rec from reimburse group by recordid )r " +
       "where l.rid=r.rid ").show()*/

    //住院人均报销比例=住院总报销/住院总花费
    /* ss.sql("select sum(r.rec)/sum(l.`all`) from " +
       "(select sum(recost) rec,recordid rid from reimburse group by recordid) r , " +
       "(select allcost `all`,recordid rid from record where flag=1) l " +
       "where r.rid=l.rid").show()*/

    //record reimburse
    //住院人均住院天数=住院总住院天数/住院总就诊人数  29
    /*ss.sql("select sum(datediff(endtime,starttime)+1)/count(recordid) from" +
      " record where flag=1").show()*/

    //住院治愈率=住院总治愈人数/住院总就诊人数 57
    /* ss.sql("select count(recordid)/(select count(recordid) from " +
       " record where flag=1 ) from  record where flag=1 and isrecovery=1").show()*/


    //record reimburse 计算每个医院的报销金额
    ss.sql("SELECT l.`hospitalid` ,SUM(r.rec) FROM\n(SELECT recordid rid ,SUM(recost) rec FROM reimburse GROUP BY recordid) r,\nrecord l WHERE r.rid=l.`recordid` GROUP BY l.`hospitalid`").show()
    //计算每个医院就诊人数
    ss.sql("SELECT l.hid,COUNT(l.rid) FROM \n(SELECT recordid rid ,hospitalid hid FROM record ) l,\n(SELECT recordid rid FROM reimburse GROUP BY recordid ) r \nWHERE l.rid=r.rid GROUP BY l.hid ;").show()
    //计算每个医院的人均花费
    ss.sql("SELECT l.hid,SUM(l.all)/COUNT(l.rid) FROM \n(SELECT allcost `all`,recordid rid ,hospitalid hid FROM record  ) l,\n(SELECT recordid rid FROM reimburse GROUP BY recordid ) r \nWHERE l.rid=r.rid GROUP BY l.hid ;").show()
    //计算每个医院人均报销费用=总报销/总就诊人数
    ss.sql("SELECT l.hid,SUM(r.rec)/COUNT(l.rid) FROM \n(SELECT allcost `all`,recordid rid ,hospitalid hid FROM record  ) l,\n(SELECT recordid rid ,SUM(recost) rec FROM reimburse GROUP BY recordid ) r \nWHERE l.rid=r.rid GROUP BY l.hid ;").show()
    //人均报销比例=总报销/总花费
    ss.sql("SELECT l.hid,SUM(r.rec)/SUM(l.all) FROM \n(SELECT allcost `all`,recordid rid ,hospitalid hid FROM record  ) l,\n(SELECT recordid rid ,SUM(recost) rec FROM reimburse GROUP BY recordid ) r \nWHERE l.rid=r.rid GROUP BY l.hid ;").show()
    //人均住院天数=总住院天数/总就诊人数
    ss.sql("SELECT l.hid,SUM(DATEDIFF(l.endtime,l.starttime)+1)/COUNT(r.rid) FROM \n(SELECT allcost `all`,recordid rid ,endtime ,starttime, hospitalid hid FROM record  ) l,\n(SELECT recordid rid ,SUM(recost) rec FROM reimburse GROUP BY recordid ) r \nWHERE l.rid=r.rid GROUP BY l.hid ;").show()
    //治愈率=总治愈人数/总就诊人数
    ss.sql("SELECT l.hid,COUNT(r.rid)/COUNT(l.rid) FROM \n(SELECT hospitalid hid, recordid rid FROM record) l LEFT JOIN\t\n(SELECT recordid rid FROM record WHERE isrecovery=1) r ON\nl.rid=r.rid GROUP BY l.hid").show()
    //住院
    ss.sql("SELECT l.hid AS '医院名称' ,COUNT(l.rid) AS '总住院人数' ,SUM(l.`allcost`) '住院总费用', SUM(r.rec) '住院总报销' ,\nSUM(l.isrecovery) AS '住院总治愈人数' ,SUM(DATEDIFF(l.endtime,l.starttime)+1) AS '住院总天数'\nFROM\n(SELECT recordid rid ,allcost ,endtime ,starttime, isrecovery ,hospitalid hid FROM record WHERE flag=1 ) l ,\n(SELECT recordid rid ,SUM(recost) rec FROM reimburse GROUP BY recordid ) r \nWHERE l.rid=r.rid GROUP BY l.hid ;").show()
//门诊
    ss.sql("SELECT l.hid AS '医院名称' ,COUNT(l.rid) AS '总门诊人数' ,SUM(l.`allcost`) '门诊总费用', SUM(r.rec) '门诊总报销' ,\nSUM(l.isrecovery) AS '门诊总治愈人数' ,SUM(DATEDIFF(l.endtime,l.starttime)+1) AS '门诊总天数'\nFROM\n(SELECT recordid rid ,allcost ,endtime ,starttime, isrecovery ,hospitalid hid FROM record WHERE flag=2 ) l ,\n(SELECT recordid rid ,SUM(recost) rec FROM reimburse GROUP BY recordid ) r \nWHERE l.rid=r.rid GROUP BY l.hid ;").show()




  }
}
