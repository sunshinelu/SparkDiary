package com.evayInfo.Inglory.Project.DocsSimilarity.CheckResult

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

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
 * Created by sunlu on 17/9/1.
 */
object CheckTitle {

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

  case class GetTitleSchema(doc1: String, doc2: String, level: String, time: String)

  def GetTitleTable(titleTable: String, year: Int, sc: SparkContext): RDD[GetTitleSchema] = {

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = year
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, titleTable) //设置输入表名

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //doc1
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //doc2
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
      val levle = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("level")) //level
      val time = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("mod")) //time
      (doc1, doc2, levle, time)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val doc1 = Bytes.toString(x._1)
        val doc2 = Bytes.toString(x._2)
        val levle = Bytes.toString(x._3)
        //时间格式转化
        val time = Bytes.toString(x._4)
        val timeL = dateFormat.parse(time).getTime
        (doc1, doc2, levle, time, timeL)
      }
      }.filter(x => x._5 <= todayL & x._5 >= nDaysAgoL).map(x => {
      GetTitleSchema(x._1, x._2, x._3, x._4)
    })

    hbaseRDD

  }

  case class GetYlzxSchema(rowkey: String, time: String)

  def GetYlzxTable(tableName: String, year: Int, sc: SparkContext): RDD[GetYlzxSchema] = {

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = year
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time

    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()

      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
      (rowkey, time)
    }
    }.filter(x => null != x._1 & null != x._2).map { x => {
      val rowkey = Bytes.toString(x._1)
      //时间格式转化
      val time = Bytes.toLong(x._2)
      (rowkey, time)
    }
    }.filter(x => x._2 <= todayL & x._2 >= nDaysAgoL).map(x => {
      val date: Date = new Date(x._2)
      val time = dateFormat.format(date)
      GetYlzxSchema(x._1, time)
    })

    hbaseRDD

  }


  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName(s"CheckTitle") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val titleTable = "docsimi_title"
    val ylzxTable = "yilan-total_webpage"

    val titleRDD = GetTitleTable(titleTable, 20, sc)
    val ylzxRDD = GetYlzxTable(ylzxTable, 20, sc)

    val titleDS = spark.createDataset(titleRDD) //500886
    val ylzxDS = spark.createDataset(ylzxRDD) //136220

    val titleDf1 = titleDS.dropDuplicates("doc1")
    println("titleDf1表中doc1的数据量为：" + titleDf1.count())
    //    titleDf1表中doc1的数据量为：101623
    println("ylzxDS的数据量为" + ylzxDS.count()) //Long = 136220

    val df1 = ylzxDS.join(titleDf1, ylzxDS("rowkey") === titleDf1("doc1"), "leftanti")
    println(df1.count()) //34597

    sc.stop()
    spark.stop()
  }
}
