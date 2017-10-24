package com.evayInfo.Inglory.SparkDiary.database.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by sunlu on 17/7/21.
  * check the data in ylzx_cnxh
  */
object checkCnxhData {

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

  def main(args: Array[String]) {

    //bulid environment
    val spark = SparkSession.builder.appName("checkCnxhData").getOrCreate()
    val sc = spark.sparkContext

    val ylzxTable = "ylzx_cnxh"
    val myID = "175786f8-1e74-4d6c-94e9-366cf1649721"

    /*
    直接在spark－shell下直接运行时会出现如下错误：
    Caused by: java.io.NotSerializableException: org.apache.hadoop.conf.Configuration
   解决方案：
   @transient
     */

    @transient val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userID")) //cREATE_BY_ID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //cREATE_TIME
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rn")) //rEQUEST_URI
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("title")) //pARAMS
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("userID")) //cREATE_BY_ID
      val id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //cREATE_TIME
      val rn = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rn")) //rEQUEST_URI
      val title = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("title")) //pARAMS
      (userID, id, rn, title)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val userID = Bytes.toString(x._1)
        val id = Bytes.toString(x._2)
        val rn = Bytes.toString(x._3)
        val title = Bytes.toString(x._4)
        (userID, id, rn, title)
      }
      }.filter(x => x._1.contains(myID))

    hbaseRDD.count()
    hbaseRDD.collect().foreach(println)

    sc.stop()
    spark.stop()

  }
}
