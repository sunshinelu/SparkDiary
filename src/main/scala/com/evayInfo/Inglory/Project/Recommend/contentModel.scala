package com.evayInfo.Inglory.Project.Recommend

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
  * Created by sunlu on 17/8/15.
  */

object contentModel {
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

  /*
  rowkey

info: level => 排名
info: simsID => 相似文章ID
info: t => 相似文章标题
info: manuallabel => 相似文章标签
info: mod => 相似文章时间
info: websitename => 相似文章网站名
info: id => urlID
   */
  case class DocsimiSchema(id: String, simsID: String, level: Double, title: String, manuallabel: String, mod: String, websitename: String)

  def getDocsimiData(tableName: String, sc: SparkContext): RDD[DocsimiSchema] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //simsID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level")) //level
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("title")) //title
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //manuallabel
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mod")) //mod
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //websitename
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //id
      val simsID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //simsID
      val level = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("level")) //level
      val title = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("title")) //title
      val manuallabel = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //manuallabel
      val mod = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("mod")) //mod
      val websitename = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //mod
      (id, simsID, level, title, manuallabel, mod, websitename)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6 & null != x._7).
      map(x => {
        val id = Bytes.toString(x._1)
        val simsID = Bytes.toString(x._2)
        val level = Bytes.toString(x._3)
        val level2 = (-0.1 * level.toInt) + 1
        val title = Bytes.toString(x._4)
        val manuallabel = Bytes.toString(x._5)
        val mod = Bytes.toString(x._6)
        val websitename = Bytes.toString(x._7)
        DocsimiSchema(id, simsID, level2, title, manuallabel, mod, websitename)
      })
    hbaseRDD
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"contentModel").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    /*
        val ylzxTable = args(0)
        val logsTable = args(1)
        val docsimiTable = args(2) //
    */
    val ylzxTable = "yilan-total_webpage"
    val logsTable = "t_hbaseSink"
    val docsimiTable = "docsimi_word2vec"

    val ylzxRDD = GetData.getYlzxRDD(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).dropDuplicates("content").drop("content")
    //    ylzxDS.printSchema()
    /*
    root
     |-- itemString: string (nullable = true)
     |-- title: string (nullable = true)
     |-- manuallabel: string (nullable = true)
     |-- time: string (nullable = true)
     |-- websitename: string (nullable = true)
     */
    val logsRDD = GetData.getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString"))
    //    logsDS.printSchema()
    /*
    root
 |-- userString: string (nullable = true)
 |-- itemString: string (nullable = true)
 |-- CREATE_TIME: long (nullable = true)
 |-- value: double (nullable = true)
     */
    val df1 = logsDS.select("userString", "itemString", "value")
    val df1_1 = logsDS.select("userString", "itemString")

    val docsimiRDD = getDocsimiData(docsimiTable, sc)
    val docsimiDS = spark.createDataset(docsimiRDD)
    //    docsimiDS.printSchema()
    /*
    root
     |-- id: string (nullable = true)
     |-- simsID: string (nullable = true)
     |-- level: double (nullable = true)
     |-- title: string (nullable = true)
     |-- manuallabel: string (nullable = true)
     |-- mod: string (nullable = true)
     |-- websitename: string (nullable = true)
     */

    val df2 = docsimiDS.select("id", "simsID", "level")

    val df3 = df1.join(df2, df1("itemString") === df2("id"), "left").
      withColumn("rating", col("value") * col("level")).drop("value").drop("level").na.drop()
    //    df3.printSchema()
    /*
    root
     |-- userString: string (nullable = true)
     |-- itemString: string (nullable = true)
     |-- id: string (nullable = true)
     |-- simsID: string (nullable = true)
     |-- rating: double (nullable = true)
     */

        df3.take(5).foreach(println)

    val df4 = df3.drop("itemString").drop("id").withColumnRenamed("simsID", "itemString").
      join(df1_1, Seq("userString", "itemString"), "leftanti").na.drop()
//    df4.printSchema()
    /*
    root
 |-- userString: string (nullable = true)
 |-- itemString: string (nullable = true)
 |-- rating: double (nullable = true)
     */
    df4.take(5).foreach(println)

    val df5 = df4.join(ylzxDS, Seq("itemString"), "left")
    df5.printSchema()
    /*

     */
    df5.take(5).foreach(println)

    sc.stop()
    spark.stop()
  }
}
