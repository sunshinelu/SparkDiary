package com.evayInfo.Inglory.Project.znpt

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Created by sunlu on 18/7/13.
 *
 * 构建Word2VecModel
 *
 * 输入数据：易览资讯标题列
 *
 * yilan-total-analysis_webpage
 *
 * 输出：Word2VecModel
 *
 */
object BuildWord2VecModel {
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

  case class YlzxSegSchema(id: Long, urlID: String, title: String, time: String, segWords: Seq[String])

  def getYlzxSeg2RDD(ylzxTable: String, year: Int, sc: SparkContext): RDD[YlzxSegSchema] = {

    //load stopwords file
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

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
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      (urlID, title, time)
    }
    }.filter(x => null != x._2 & null != x._3 ).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        //时间格式转化
        val time_1 = Bytes.toLong(x._3)


        (urlID_1, title_1, time_1)
      }
      }.filter(x => x._2.length > 1 ).filter(x => x._3 <= todayL & x._3 >= nDaysAgoL).
      map(x => {
        val urlID = x._1
        val title = x._2
        val date: Date = new Date(x._3)
        val time = dateFormat.format(date)

        //使用ansj分词
        val segWords = ToAnalysis.parse(title).toArray.map(_.toString.split("/")).
          filter(_.length >= 2).map(_ (0)).toList.
          filter(word => word.length >= 2 & !stopwords.value.contains(word)).toSeq
        (urlID, title, time, segWords)
      }).zipWithUniqueId().map(x => {
      val id = x._2
      val urlID = x._1._1
      val title = x._1._2
      val time = x._1._3
      val segWords = x._1._4
      YlzxSegSchema(id, urlID, title, time, segWords)
    }).filter(x => null != x.segWords).filter(_.segWords.size > 1) //.randomSplit(Array(0.1,0.9))(0)

    hbaseRDD

  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(s"BuildWord2VecModel").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = args(0)
//    val ylzxTable = "yilan-total-analysis_webpage"
    val featuresNum = args(1)
    val word2vec_model_path = ""



    sc.stop()
    spark.stop()
  }

}
