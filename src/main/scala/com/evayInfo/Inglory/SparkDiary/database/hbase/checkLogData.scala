package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sunlu on 17/7/13.
  * 查看t_hbaseSink表中是否有标签为“双创”的数据
  *
  * //LOG_ID, TYPE, TITLE, CREATE_BY, CREATE_BY_ID, CREATE_TIME, REMOTE_ADDR, USER_AGENT, REQUEST_URI, METHOD, PARAMS
  * //编号, 日志类型(1：接入日志；2：错误日志), 日志标题, 创建者, 登录人ID, 创建时间, 操作IP地址, 用户代理, 请求URI, 操作方式, 操作提交的数据
  *
  */
object checkLogData {
  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("checkLogData").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = "yilan-total_webpage"
    val logsTable = "t_hbaseSink"
    val myID = "175786f8-1e74-4d6c-94e9-366cf1649721"
    val gID = "f4d21395-f9a7-4a40-8e63-aa04c2f73a5d"

    val ylzxRDD = getYlzxRDD(ylzxTable, sc)
    val ylzxDF = spark.createDataset(ylzxRDD)

    val logsTimeRDD = getLogsTimeRDD(logsTable, sc)
    val logsTimeDS = spark.createDataset(logsTimeRDD)

    val ds0 = logsTimeDS.filter(col("time").contains("2017-07-13")).
      filter(col("REQUEST_URI").contains( """search/getContentById.do""")).select($"PARAMS").
      collect() //select("time").sort(col("time").desc).show(7)

    val logsRDD = getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD)
    val ds1 = logsDS.filter(col("userID") === myID).filter(col("time").contains("2017-07-13"))
    val ds2 = ds1.join(ylzxDF, Seq("id"), "left").select("userID", "id", "manuallabel").na.drop(Array("manuallabel")) //ueserID, id, manuallabel
    val ds3_ColName = Seq("ueserID", "id", "manuallabel", "label")
    val ds3 = ds2.as[(String, String, String)].flatMap {
      case (ueserID, id, manuallabel) => manuallabel.split(";").map((ueserID, id, manuallabel, _))
    }.toDF(ds3_ColName: _*)
    val ds4 = ds3.filter($"label".contains("双创"))
    ds3.filter($"manuallabel".contains("双创")).show()

    val ds5 = ylzxDF.filter($"manuallabel".contains("双创"))


  }


  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  case class LogView(CREATE_BY_ID: String, REQUEST_URI: String, PARAMS: String, time: String)

  case class LogView2(userID: String, id: String, time: String)

  case class LogView3(userID: String, time: String)

  case class LyzxView(id: String, manuallabel: String)


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
        LogView(userID, requestURL, parmas, creatTimeS)
      }
      }.filter(x => x.REQUEST_URI.contains( """search/getContentById.do""")).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        //        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val reg2 =
          """id=\S*,|id=\S*}""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "").replace("})", "")
        LogView2(userID, urlString, x.time)
      }).filter(_.id.length >= 10)

    hbaseRDD
  }

  def getYlzxRDD(ylzxTable: String, sc: SparkContext): RDD[LyzxView] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "yilan-ywk_webpage")
    //指定输出格式和输出表名
    //conf.set(TableOutputFormat.OUTPUT_TABLE, args(1)) //设置输出表名，与输入是同一个表t_userProfileV1
    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      (urlID, manuallabel)
    }
    }.filter(x => null != x._2).
      map(x => {
        val urlID_1 = Bytes.toString(x._1)
        val manuallabel_1 = Bytes.toString(x._2)
        LyzxView(urlID_1, manuallabel_1)
      }
      ).filter(_.manuallabel.length >= 2)

    hbaseRDD

  }

  def getLogsTimeRDD(logsTable: String, sc: SparkContext): RDD[LogView] = {

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
        LogView(userID, requestURL, parmas, creatTimeS)
      }
      }
    hbaseRDD
  }


}
