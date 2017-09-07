package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

/*
查看商务厅推荐中的数据
查看标签为“商务”的数据

 */
object checkSWTdata {

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class ylzxSchema(itemString: String, title: String, segWords: Seq[String], manuallabel: String, time: Long)

  def getYlzxRDD(ylzxTable: String, year: Int, sc: SparkContext): RDD[ylzxSchema] = {

    //load stopwords file
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    // 获取时间
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

    //val ylzxTable = "yilan-total_webpage"
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) // 内容列
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      (urlID, title, content, manuallabel, time)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        //使用ansj分词
        val segWords = ToAnalysis.parse(content_1).toArray.map(_.toString.split("/")).
          filter(_.length >= 2).map(_ (0)).toList.
          filter(word => word.length >= 2 & !stopwords.value.contains(word)).toSeq

        val manuallabel_1 = Bytes.toString(x._4)
        //时间格式转化
        val time = Bytes.toLong(x._5)
        ylzxSchema(urlID_1, title_1, segWords, manuallabel_1, time)
      }
      }.filter(x => {
      x.title.length >= 2
    }).filter(x => x.time >= nDaysAgoL).filter(_.segWords.size > 1).filter(x => x.manuallabel.contains("商务"))

    hbaseRDD
  }

  case class ylzxSchema2(itemString: String, title: String, manuallabel: String, time: Long)

  def getYlzxRDD2(ylzxTable: String, sc: SparkContext): RDD[ylzxSchema2] = {

    // 获取时间
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
    val N = 1
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime


    //val ylzxTable = "yilan-total_webpage"
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      (urlID, title, manuallabel, time)
    }
    }.//filter(x => null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = if (null != x._2) (Bytes.toString(x._2)) else ""
        val manuallabel_1 = if (null != x._3) (Bytes.toString(x._3))  else ""
        //时间格式转化
        val time = if (null != x._4) (Bytes.toLong(x._4)) else (0L) //toString(x._4).toLong
        ylzxSchema2(urlID_1, title_1, manuallabel_1, time)
      }
      }.filter(x => x.manuallabel.contains("商务"))
    /*.filter(x => {
    x.title.length >= 2
  }).filter(x => x.time >= nDaysAgoL).filter(x => x.manuallabel.contains("商务"))
*/
    hbaseRDD
  }


  def main(args: Array[String]): Unit = {
    // 不输出日志
    SetLogger

    /*
    1. bulid spark environment
     */

    val sparkConf = new SparkConf().setAppName(s"swt: ContentRecomm").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    2. get data
     */

    val ylzxTable = "yilan-total_webpage"

    val ylzxRDD = getYlzxRDD(ylzxTable, 20, sc)
    val ylzxDS = spark.createDataset(ylzxRDD)

    ylzxDS.filter(col("itemString") === "3f8ed9a1-541b-4fb1-97f8-d2987935ab3a").collect().foreach(println)
    ylzxDS.filter(col("itemString") === "1cbff174-2194-4556-90af-a11e4656c2d4").collect().foreach(println)
    println("===============================")
    val ylzxRDD2 = getYlzxRDD2(ylzxTable, sc)
    val ylzxDS2 = spark.createDataset(ylzxRDD2)

    ylzxDS2.filter(col("itemString") === "3f8ed9a1-541b-4fb1-97f8-d2987935ab3a").collect().foreach(println)
    ylzxDS2.filter(col("itemString") === "1cbff174-2194-4556-90af-a11e4656c2d4").collect().foreach(println)

    /*
        get 'yilan-total_webpage','1cbff174-2194-4556-90af-a11e4656c2d4'
        get 'yilan-total_webpage','3f8ed9a1-541b-4fb1-97f8-d2987935ab3a'

    count 'yilan-total_webpage'

        */

    sc.stop()
    spark.stop()

  }

}
