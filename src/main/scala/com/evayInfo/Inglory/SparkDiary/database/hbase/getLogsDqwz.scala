package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/1/15.
 * 读取用户行为日志获取用户在“猜你喜欢”模块的点击情况
 * 读取猜你喜欢推荐结果（此方法不可行，因为未记录历史推荐数据）
 */
object getLogsDqwz {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  case class LogView(CREATE_BY_ID: String, REQUEST_URI: String, PARAMS: String, time: String, timeL: Long)
  case class LogView2(userID: String, id: String, time: String, timeL: Long)

  def getLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView2] = {

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, logsTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
      val creatTime = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
      val requestURL = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
      val parmas = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
      (userID, creatTime, requestURL, parmas)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val userID = Bytes.toString(x._1)
        val creatTime = Bytes.toString(x._2)
        //定义时间格式
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val creatTimeD = dateFormat.parse(creatTime)
        val creatTimeS = dateFormat.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, requestURL, parmas, creatTimeS, creatTimeL)
      }
      }.filter(x => x.REQUEST_URI.contains( """getContentById.do""")).
      filter(_.PARAMS.toString.length >= 10).
      filter(_.PARAMS.contains("猜你喜欢")).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        //        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val reg2 =
          """id=\S*,|id=\S*}""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "").replace("})", "")
        LogView2(userID, urlString, x.time, x.timeL)
      }).filter(_.id.length >= 10).filter(_.userID.length >= 5)

    hbaseRDD
  }

  case class CnxhSchema(userID: String, id: String, sysTime: String, sysTimeL:Long)
  def getCnxhRDD(logsTable: String, sc: SparkContext): RDD[CnxhSchema] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, logsTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userID")) //userID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sysTime")) //sysTime
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("userID")) //userID
      val id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //id
      val sysTime = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("sysTime")) //sysTime
      (userID, id, sysTime)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3).
      map { x => {
        val userID = Bytes.toString(x._1)
        val id = Bytes.toString(x._2)
        val creatTime = Bytes.toString(x._3)
        //定义时间格式
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val creatTimeD = dateFormat.parse(creatTime)
        val creatTimeS = dateFormat.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        CnxhSchema(userID, id, creatTimeS, creatTimeL)
      }
      }
    hbaseRDD
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"getLogsDqwz").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val logsTable = "t_hbaseSink"
    val logsRDD = getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD)



    logsDS.persist()
    println("logsDS number is: " + logsDS.count())
    // res24: Long = 655

    logsDS.distinct.count
//    res25: Long = 654

    logsDS.select("userID","id").distinct().count
//res26: Long = 467

    logsDS.select("userID","id","timeL").distinct().count
//res27: Long = 472

    logsDS.printSchema()
    /*
    root
 |-- userID: string (nullable = true)
 |-- id: string (nullable = true)
 |-- time: string (nullable = true)
 |-- timeL: long (nullable = true)
     */

//    val df1 = logsDS.withColumn("tag", lit(1)).groupBy("userID","timeL").agg(sum("tag"))
//    df1.show(false)
//    val df2 = df1.agg(min("sum(tag)"), max("sum(tag)"))
//    df2.show(false)

    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var month = dateFormat.format(cal.getTime())
    val monthL = dateFormat.parse(month).getTime

    val df4 = logsDS.filter($"timeL" >= monthL)
    df4.count()

    val df5 = df4.withColumn("tag", lit(1)).
      groupBy("userID","timeL").agg(sum("tag"))
    df5.show(false)
//     val df6 = df5.agg(min("sum(tag)"), max("sum(tag)"))

    val clickRate = df5.filter($"sum(tag)" <= 5).withColumn("clickRate", $"sum(tag)" / 5).agg(mean("clickRate")).first().get(0)
// 0.5428571428571429

    println("易览资讯－猜你喜欢模型点击率为：" + clickRate)

/*
    val cnxhTable = "ylzx_cnxh"
    val cnxhRDD = getCnxhRDD(cnxhTable, sc)
    val cnxhDS = spark.createDataset(cnxhRDD)
    cnxhDS.persist()
    println("cnxhDS number is: " + cnxhDS.count())

    val df3 = logsDS.join(cnxhDS, Seq("userID", "id"), "left")
    df3.printSchema()
    df3.persist()
    df3.show(false)
    /*
    root
 |-- userID: string (nullable = true)
 |-- id: string (nullable = true)
 |-- time: string (nullable = true)
 |-- timeL: long (nullable = true)
 |-- sysTime: string (nullable = true)
 |-- sysTimeL: long (nullable = true)
     */

*/

    sc.stop()
    spark.stop()
  }

}
