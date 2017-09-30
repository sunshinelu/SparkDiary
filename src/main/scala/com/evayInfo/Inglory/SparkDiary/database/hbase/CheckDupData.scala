package com.evayInfo.Inglory.SparkDiary.database.hbase

import org.ansj.app.keyword.KeyWordComputer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/9/30.
 *
 * 使用标题＋5个关键词查看分析表中的重复数据
 *
 */
object CheckDupData {

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

  case class YlzxSchema(itemString: String, title: String, websitename: String, combinedContent: String, columnId: String)

  def getYlzxYRDD(ylzxTable: String, sc: SparkContext): RDD[YlzxSchema] = {

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //websitename
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("column_id"))
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //websitename列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //content列
      val column_id = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("column_id"))
      (urlID, title, webName, content, column_id)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2).replaceAll("【.+】", "")
        val websitename_1 = Bytes.toString(x._3)
        val content_1 = Bytes.toString(x._4).replace("&nbsp;", "").replaceAll("\\uFFFD", "").replaceAll("([\\ud800-\\udbff\\udc00-\\udfff])", "")
        val column_id = Bytes.toString(x._5)
        //每篇文章提取5个关键词
        val kwc = new KeyWordComputer(5)
        val keywords = kwc.computeArticleTfidf(title_1, content_1).toArray.map(_.toString.split("/")).
          filter(_.length >= 2).map(_ (0)).toList.mkString(" ")
        val combinedWords = title_1 + keywords
        YlzxSchema(urlID_1, title_1, websitename_1, combinedWords, column_id)
      }
      }.filter(_.title.length >= 2)

    hbaseRDD

  }

  def main(args: Array[String]) {
    SetLogger


    // bulid spark environment
    val sparkConf = new SparkConf().setAppName(s"CheckDupData").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = "yilan-total-analysis_webpage"

    val ylzxRDD = getYlzxYRDD(ylzxTable, sc)
    val ylzxDf = spark.createDataset(ylzxRDD)
    val ylzxDfDup = ylzxDf.withColumn("value", lit(1)).groupBy("title", "combinedContent", "columnId").
      agg(sum("value")).filter($"sum(value)" > 1)

    ylzxDfDup.show(false) //8432

    val dupDF = ylzxDfDup.select("title", "columnId").join(ylzxDf, Seq("title", "columnId"), "left").
      select("itemString", "title", "columnId")
    //    dupDF.coalesce(1).write.mode(SaveMode.Overwrite).csv("file:///root/lulu/Workspace/spark/test/dup.csv")

    dupDF.write.mode(SaveMode.Overwrite).csv("/personal/sunlu/ylzx/dup")

    val t1 = ylzxDf.withColumn("value", lit(1)).groupBy("title").
      agg(sum("value")).filter($"sum(value)" > 1)
    t1.collect().foreach(println)

    sc.stop()
    spark.stop()

  }

}
