package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat

import org.ansj.app.keyword.KeyWordComputer
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/10/18.
 * 查看相关文章中计算出来的结果
 *
 * spark-shell --master yarn --num-executors 4 --executor-cores  2 --executor-memory 4g --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar

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

  case class GetYlzxSchema(rowkey: String, title: String, content: String)

  def GetYlzxTable(tableName: String, sc: SparkContext): RDD[GetYlzxSchema] = {

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //title

    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
      (rowkey, title, content)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3).map { x => {
      val rowkey = Bytes.toString(x._1)
      val title = Bytes.toString(x._2)
      val content = Bytes.toString(x._3)
      GetYlzxSchema(rowkey, title, content)
    }
    }
    hbaseRDD

  }


  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"checkXgwzData") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val xgwzTable = "ylzx_xgwz"
    val ylzxTable = "yilan-total-analysis_webpage"

    val xgwzRDD = GetXgwzTable(xgwzTable, sc)
    val xgwzDS = spark.createDataset(xgwzRDD)

    val ylzxRDD = GetYlzxTable(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD)

    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    //定义UDF
    //分词、停用词过滤
    def segWordsFunc(title: String, content: String): String = {
      //每篇文章提取5个关键词
      val kwc = new KeyWordComputer(5)
      val keywords = kwc.computeArticleTfidf(title, content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.mkString(" ")
      val combinedWords = title + " keywords " + keywords
      val seg = NlpAnalysis.parse(combinedWords).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.value.contains(word)).toSeq
      val result = seg.mkString(" ")
      combinedWords
    }

    val segWordsUDF = udf((title: String, content: String) => segWordsFunc(title, content))
    val segDF = ylzxDS.withColumn("segWords", segWordsUDF($"title", $"content")).drop("content").drop("title") //.filter(!$"segWords".contains("null"))

    val df1 = xgwzDS.join(segDF, xgwzDS("doc1") === segDF("rowkey")).
      withColumnRenamed("rowkey", "doc1ID").withColumnRenamed("segWords", "doc1Title")
    val df2 = df1.join(segDF, df1("doc2") === segDF("rowkey")).
      withColumnRenamed("rowkey", "doc2ID").withColumnRenamed("segWords", "doc2Title")

    //df2.persist(StorageLevel.MEMORY_AND_DISK_SER)

    df2.filter($"simiScore" >= 0.95).select("doc1Title", "doc2Title", "simiScore").show(false)

    df2.filter($"simiScore" >= 0.8 && $"simiScore" <= 0.9).select("doc1Title", "doc2Title", "simiScore").show(false)

    df2.filter($"simiScore" >= 0.7 && $"simiScore" <= 0.8).select("doc1Title", "doc2Title", "simiScore").show(false)


    df2.filter($"simiScore" === 0).select("doc1Title", "doc2Title", "simiScore").show(false)
    df2.filter($"simiScore" > 0.0).select(min($"simiScore")).show(false)

    println("count xgwzDS: " + xgwzDS.count())
    //996714


    println("count xgwzDS: " + xgwzDS.filter($"simiScore" <= 0.5).count())
    //    count xgwzDS: 233935

    println("count xgwzDS: " + xgwzDS.filter($"simiScore" <= 0.6).count())
    //    count xgwzDS: 319860

    println("count xgwzDS: " + xgwzDS.filter($"simiScore" <= 0.7).count())
    //    count xgwzDS: 466076

    println("count xgwzDS: " + xgwzDS.filter($"simiScore" <= 0.8).count())
    //    count xgwzDS: 698989

    println("count xgwzDS: " + xgwzDS.filter($"simiScore" <= 0.9).count())
    //    count xgwzDS: 935193

    println("count xgwzDS: " + xgwzDS.filter($"simiScore" <= 0.95).count())
    //    count xgwzDS: 989698


    sc.stop()
    spark.stop()

  }
}
