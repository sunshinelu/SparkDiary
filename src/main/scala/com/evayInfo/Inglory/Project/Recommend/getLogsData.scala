package com.evayInfo.Inglory.Project.Recommend

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/8/1.
 * 提取 t_hbaseSink 表中的部分数据用来测试推荐算法
  *  将数据保存在 t_logs_sun 表中
  *  count 't_logs_sun'
  *  => 26488
 */
object getLogsData {

  case class LogView(rowkey: String, CREATE_BY_ID: String, CREATE_TIME: String, REQUEST_URI: String, PARAMS: String)


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

  def getLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView] = {

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
      (rowkey, userID, creatTime, requestURL, parmas)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val rowkey = Bytes.toString(x._1)
        val userID = Bytes.toString(x._2)
        val creatTime = Bytes.toString(x._3)
        val requestURL = Bytes.toString(x._4)
        val parmas = Bytes.toString(x._5)
        LogView(rowkey, userID, creatTime, requestURL, parmas)
      }
      }.filter(x => x.REQUEST_URI.contains("search/getContentById.do") || x.REQUEST_URI.contains("like/add.do") ||
      x.REQUEST_URI.contains("favorite/add.do") || x.REQUEST_URI.contains("favorite/delete.do")
    ).filter(_.PARAMS.toString.length >= 10)

    hbaseRDD
  }

  def main(args: Array[String]) {

    SetLogger

    // 配置spark环境
    val sparkConf = new SparkConf().setAppName(s"getLogsData").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val inputTable = "t_hbaseSink"
    val outputTable = "t_logs_sun"

    // 设置hbase configure
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.37.21,192.168.37.22,192.168.37.23")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.master", "192.168.37.22:60000", "192.168.37.23:60000")

    hbaseConf.addResource("mapred-site.xml")
    hbaseConf.addResource("yarn-site.xml")
    hbaseConf.addResource("hbase-site.xml")

    // 指定输入格式和输出表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, inputTable)
    //指定输出格式和输出表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名

    //判断HBAE表是否存在，如果存在则删除表，然后新建表
    val hAdmin = new HBaseAdmin(hbaseConf)
    if (hAdmin.tableExists(outputTable)) {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    val htd = new HTableDescriptor(TableName.valueOf(outputTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)

    //创建job
    val jobConf = new Configuration(hbaseConf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    // 获取日志数据
    val logsRDD = getLogsRDD(inputTable, sc)

//    println(logsRDD.count())

    // 保存日志数据
    logsRDD.map(x => {
      val key = Bytes.toBytes(x.rowkey)
      val put = new Put(key)
      put.add(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID"), Bytes.toBytes(x.CREATE_BY_ID)) //CREATE_BY_ID
      put.add(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME"), Bytes.toBytes(x.CREATE_TIME)) //CREATE_TIME
      put.add(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI"), Bytes.toBytes(x.REQUEST_URI.toString)) //REQUEST_URI
      put.add(Bytes.toBytes("info"), Bytes.toBytes("pARAMS"), Bytes.toBytes(x.PARAMS.toString)) //PARAMS

      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(jobConf)


    sc.stop()
    spark.stop()


  }

}
