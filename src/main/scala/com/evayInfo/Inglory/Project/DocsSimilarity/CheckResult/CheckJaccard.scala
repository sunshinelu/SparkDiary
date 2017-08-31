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
 * Created by sunlu on 17/8/31.
 * 在spark－shell下进行测试：
 * spark-shell --master yarn --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar
 *
 *
 */
object CheckJaccard {
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

  case class GetJaccardSchema(doc1: String, doc2: String, level: String, time: String)

  def GetJaccardTable(jaccardTable: String, year: Int, sc: SparkContext): RDD[GetJaccardSchema] = {

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
    conf.set(TableInputFormat.INPUT_TABLE, jaccardTable) //设置输入表名

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
      GetJaccardSchema(x._1, x._2, x._3, x._4)
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
    val sparkConf = new SparkConf().setAppName(s"CheckJaccard") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val jaccardTable = "docsimi_jaccard"
    val ylzxTable = "yilan-total_webpage"

    val jaccardRDD = GetJaccardTable(jaccardTable, 20, sc)
    val ylzxRDD = GetYlzxTable(ylzxTable, 20, sc)

    val jaccardDS = spark.createDataset(jaccardRDD) //252262
    val ylzxDS = spark.createDataset(ylzxRDD) //134503

    val jaccardDf1 = jaccardDS.dropDuplicates("doc1")

    println("jaccardDf1表中doc1的数据量为：" + jaccardDf1.count())
    //jaccardDf1表中doc1的数据量为：52046
    println("ylzxDS的数据量为" + ylzxDS.count()) //Long = 134503

    val df1 = ylzxDS.join(jaccardDf1, ylzxDS("rowkey") === jaccardDf1("doc1"), "leftanti")
    println(df1.count()) //82457
    df1.take(5).foreach(println)
    /*
  [0010c592-3499-479d-9999-849faf870164,2017-07-25]
  [017f74bb-9114-4ac8-814f-5df2f460e956,2017-07-22]
  [022f5432-c8bd-4123-91fd-18c5d267b038,2013-07-04]
  [030d0a53-c996-42dd-9c9c-39514c4464a2,2017-08-07]
  [03e73f5f-7896-44f8-860e-8102a220b1cf,2017-05-02]
     */

    val df2 = jaccardDS.filter($"doc1" === "03e73f5f-7896-44f8-860e-8102a220b1cf") // 0

    val df3 = jaccardDS.filter($"doc2" === "03e73f5f-7896-44f8-860e-8102a220b1cf") // 0


    sc.stop()
    spark.stop()

  }

}
