package com.evayInfo.Inglory.SparkDiary.database.hbase

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
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: sunlu
  * @Date: 2019-03-15 14:19
  * @Version 1.0
  * 说明：
  * 读取HBase中relation表的部分数据，并提取前100条，保存到writeHBase_test表中。
  * 如果writeHBase_test表存在则不做任何操作，如果writeHBase_test表不存在则新建表
  */
object WriteHBaseTable {


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


  def main(args: Array[String]): Unit = {

    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"WriteHBaseTable").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


    val ipt_table = "relation" // 输入表表名
    val conf_hbase = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf_hbase.set(TableInputFormat.INPUT_TABLE, ipt_table) //设置输入表名


    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("source_id")) //source_id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("source_name")) //source_name
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("target_id")) //target_id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("target_name")) //target_name
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("relation")) //relation
    conf_hbase.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf_hbase, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val hbaseRDD = hBaseRDD.map{ case (k, v) => {
      val rowkey = k.get()
      val source_id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("source_id")) //source_id
      val source_name = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("source_name")) //source_name
      val target_id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("target_id")) //target_id
      val target_name = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("target_name")) //target_name
      val relation = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("relation")) //relation
      (source_id, source_name, target_id, target_name,relation)
    }}//.take(100)


    val opt_table = "writeHBase_test" // 输出表表名

    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(conf_hbase)
    if (!hadmin.isTableAvailable(opt_table)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(opt_table))
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes()))
      hadmin.createTable(tableDesc)
    } else {
      print("Table  Exists!  not Create Table")
    }
    /*
        //如果outputTable表存在，则删除表；如果不存在则新建表。
        val hAdmin = new HBaseAdmin(conf)
        if (hAdmin.tableExists(outputTable)) {
          hAdmin.disableTable(outputTable)
          hAdmin.deleteTable(outputTable)
        }
        //    val htd = new HTableDescriptor(outputTable)
        val htd = new HTableDescriptor(TableName.valueOf(outputTable))
        htd.addFamily(new HColumnDescriptor("info".getBytes()))
        hAdmin.createTable(htd)
    */

    //指定输出格式和输出表名
    conf_hbase.set(TableOutputFormat.OUTPUT_TABLE, opt_table) //设置输出表名

    val jobConf = new Configuration(conf_hbase)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)


//    val resultRDD = sc.parallelize(hbaseRDD).map{x => (x(0),x(1),x(2),x(3),x(4))}
    val resultRDD = hbaseRDD//.map{x => (x(0),x(1),x(2),x(3),x(4))}
    resultRDD.map{ x=> {
      val id = UUID.randomUUID().toString().toLowerCase()
      val key = Bytes.toBytes(id)
      val put = new Put(key)
      put.add(Bytes.toBytes("info"), Bytes.toBytes("source_id"), Bytes.toBytes(x._1.toString)) //source_id
      put.add(Bytes.toBytes("info"), Bytes.toBytes("source_name"), Bytes.toBytes(x._2.toString)) //source_name
      put.add(Bytes.toBytes("info"), Bytes.toBytes("target_id"), Bytes.toBytes(x._3.toString)) //target_id
      put.add(Bytes.toBytes("info"), Bytes.toBytes("target_name"), Bytes.toBytes(x._4.toString)) //target_name
      put.add(Bytes.toBytes("info"), Bytes.toBytes("relation"), Bytes.toBytes(x._5.toString)) //relation

      (new ImmutableBytesWritable, put)
    }
    }.saveAsNewAPIHadoopDataset(jobConf)


    sc.stop()
    spark.stop()
  }

}
