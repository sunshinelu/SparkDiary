package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/4/23.
 *
 * 查看用户日志表中USERIP的情况
 *
 * 在spark-shell下输入命令
 * spark-shell --master yarn
 *
 * HBASE表结构
 *
 *
  `LOG_ID` varchar(36) CHARACTER SET utf8 NOT NULL COMMENT '编号',
  `TYPE` char(1) CHARACTER SET utf8 DEFAULT '1' COMMENT '日志类型  1：接入日志；2：错误日志',
  `TITLE` varchar(255) CHARACTER SET utf8 DEFAULT  COMMENT '日志标题',
  `CREATE_BY` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '创建者',
  `CREATE_BY_ID` varchar(36) CHARACTER SET utf8 DEFAULT NULL COMMENT '登录人ID',
  `CREATE_TIME` varchar(30) CHARACTER SET utf8 DEFAULT NULL COMMENT '创建时间',
  `REMOTE_ADDR` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '操作IP地址',
  `USER_AGENT` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '用户代理',
  `REQUEST_URI` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '请求URI',
  `METHOD` varchar(5) CHARACTER SET utf8 DEFAULT NULL COMMENT '操作方式',
  `PARAMS` text CHARACTER SET utf8 COMMENT '操作提交的数据',
 *
 */
object checkUserIp {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
  case class LogView(CREATE_BY_ID: String, REQUEST_URI: String, PARAMS: String, time: String, timeL: Long, remoteAddr: String)

  def main(args: Array[String]) {
    //bulid environment
    val spark = SparkSession.builder.appName("checkUserIp").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val logsTable = "t_hbaseSink"

//    在spark-shell下输入命令时 需要使用 @transient
    @transient val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, logsTable) //设置输入表名

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rEMOTE_ADDR")) // uSER_AGENT
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val logsRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
      val creatTime = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
      val requestURL = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
      val parmas = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
      val userAgent = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rEMOTE_ADDR")) //pARAMS
      (userID, creatTime, requestURL, parmas,userAgent)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4 & null != x._5).
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
        val remoteAddr = Bytes.toString(x._5)

      LogView(userID, requestURL, parmas, creatTimeS, creatTimeL,remoteAddr)
    }
    }

    val logsDS = spark.createDataset(logsRDD)
    val df1 = logsDS.select("time", "remoteAddr").orderBy(col("time").asc)
    df1.persist(StorageLevel.MEMORY_AND_DISK)

    val filePath = "/personal/sunlu/Project/remoteAddr"
//    保存到csv文件中，含表头
    df1.write.mode("overwrite").
      option("header", true).option("delimiter", "#").
      format("csv").save(path = filePath)

    //    保存到csv文件中，不含表头
    val filePath_noHeader = "/personal/sunlu/Project/remoteAddr_noheader"
    df1.write.mode("overwrite").
      option("header", false).option("delimiter", "#").
      format("csv").save(path = filePath_noHeader)

    sc.stop()
    spark.stop()
  }
}
