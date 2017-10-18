package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/10/18.
 */
object checkXgwzData {


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

  case class XGWZschema(doc1: String, doc2: String, simiScore: Double, level: String, time: String)

  def GetXgwzTable(titleTable: String, sc: SparkContext): RDD[XGWZschema] = {

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, titleTable) //设置输入表名

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //doc1
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //doc2
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsScore")) //simsScore
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level")) //level
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mod")) //time

    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val doc1 = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //doc1
      val doc2 = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //doc2
      val simsScore = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsScore")) //level
      val levle = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("level")) //level
      val time = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("mod")) //time
      (doc1, doc2, simsScore, levle, time)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4 & null != x._5).
      map { x => {
        val doc1 = Bytes.toString(x._1)
        val doc2 = Bytes.toString(x._2)
        val simsScore = Bytes.toString(x._3).toDouble
        val levle = Bytes.toString(x._4)
        //时间格式转化
        val time = Bytes.toString(x._5)
        //        val timeL = dateFormat.parse(time).getTime
        XGWZschema(doc1, doc2, simsScore, levle, time)
      }
      }

    hbaseRDD

  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"CheckTitle") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val xgwzTable = "ylzx_xgwz"
    val xgwzRDD = GetXgwzTable(xgwzTable, sc)
    val xgwzDS = spark.createDataset(xgwzRDD)

    println("count xgwzDS: " + xgwzDS.count())
    println("count xgwzDS: " + xgwzDS.filter($"simiScore" <= 0.5).count())



    sc.stop()
    spark.stop()

  }
}
