package com.evayInfo.Inglory.Project.DocsSimilarity

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/8/20.
 */
object rewriteHbase {

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

  case class YlzxSchema(urlID: String, title: String, content: String, label: String, time: Long, websitename: String)

  case class docSimsSchema(doc1: String, doc2: String, sims: Double)


  def getYlzxRDD(ylzxTable: String, sc: SparkContext): RDD[YlzxSchema] = {

    val hbaseConf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    /*
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.37.21,192.168.37.22,192.168.37.23")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.master", "192.168.37.22:60000", "192.168.37.23:60000")
*/
    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("appc"))

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //内容列
      val label = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //网站名列
      val appc = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("appc")) //appc
      (urlID, title, content, label, time, webName, appc)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._5 & null != x._6 & null != x._7).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = Bytes.toString(x._2)
        val content_1 = Bytes.toString(x._3)
        val label_1 = Bytes.toString(x._4)
        //时间格式转化
        val time_1 = Bytes.toLong(x._5)
        val websitename_1 = Bytes.toString(x._6)

        YlzxSchema(urlID_1, title_1, content_1, label_1, time_1, websitename_1)
      }
      }.filter(x => x.urlID.length > 1 & x.content.length > 50)

    hbaseRDD

  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"docsSimilarity").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val ylzxTable = "yilan-total_webpage"
    val reWriteTable = "t_ylzx_sun"
    val ylzxRDD = getYlzxRDD(ylzxTable, sc).repartition(20)

    val hbaseConf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名
    //指定输出格式和输出表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, reWriteTable) //设置输出表名

    /*
        hbaseConf.set("hbase.zookeeper.quorum", "192.168.37.21,192.168.37.22,192.168.37.23")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConf.set("hbase.master", "192.168.37.22:60000", "192.168.37.23:60000")
    */
    /*
    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(hbaseConf)
    if (!hadmin.isTableAvailable(docSimiTable)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(docSimiTable))
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes()))
//      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }
*/

    //如果outputTable表存在，则删除表；如果不存在则新建表。=> START
    val hAdmin = new HBaseAdmin(hbaseConf)
    if (hAdmin.tableExists(reWriteTable)) {
      hAdmin.disableTable(reWriteTable)
      hAdmin.deleteTable(reWriteTable)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(reWriteTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    //    htd.addFamily(new HColumnDescriptor("f".getBytes()))
    hAdmin.createTable(htd)
    //如果outputTable表存在，则删除表；如果不存在则新建表。=> OVER


    val jobConf = new Configuration(hbaseConf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)


    ylzxRDD.map { x => {
      //sims, rn, url2id,  title, label, time, websitename, urlID
      val rowkey = UUID.randomUUID().toString().toLowerCase()
      val key = Bytes.toBytes(rowkey)
      val put = new Put(key)
      //urlID: String, title: String, content: String, label: String, time: String, websitename: String
      put.add(Bytes.toBytes("info"), Bytes.toBytes("urlid"), Bytes.toBytes(x.urlID)) //urlID
      put.add(Bytes.toBytes("info"), Bytes.toBytes("title"), Bytes.toBytes(x.title)) //title
      put.add(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(x.content)) //content
      put.add(Bytes.toBytes("info"), Bytes.toBytes("label"), Bytes.toBytes(x.label)) //label
      put.add(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(x.time)) //time
      put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x.websitename)) //websitename

      (new ImmutableBytesWritable, put)
    }
    }.saveAsNewAPIHadoopDataset(jobConf)


    sc.stop()
    spark.stop()

  }


}
