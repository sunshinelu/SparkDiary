package com.evayInfo.Inglory.Project.RenCai.DemoTest


import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
将HBASE里面的数据保存到mysql里面
 */
object HBaseToMySQL {

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

  case class col_schema(source_id :String,source_name:String,target_id:String,
    target_name:String,relation:String,relation_object:String,weight:Double,create_time:String,update_time:String)

  def main(args: Array[String]): Unit = {

    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"HBaseToMySQL").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val table_name_ipt = "relation"
//    val table_name_ipt = "relation_shuxi"
    val conf_hbase = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf_hbase.set(TableInputFormat.INPUT_TABLE, table_name_ipt) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("source_id")) //source_id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("source_name")) //source_name
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("target_id")) //target_id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("target_name")) //target_name
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("relation")) //relation
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("relation_object")) //relation_object
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weight")) //weight
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("create_time")) //create_time
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("update_time")) //update_time
    conf_hbase.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf_hbase, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val id = k.get()
      val source_id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("source_id")) //source_id
      val source_name = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("source_name")) //source_name
      val target_id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("target_id")) //target_id
      val target_name = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("target_name")) //target_name
      val relation = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("relation")) //relation
      val relation_object = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("relation_object")) //relation_object
      val weight = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("weight")) //weight
      val create_time = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("create_time")) //create_time
      val update_time = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("update_time")) //update_time
      (source_id, source_name,target_id,target_name,relation, relation_object,weight,create_time,update_time)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val source_id = Bytes.toString(x._1)
        val source_name = Bytes.toString(x._2)
        val target_id = Bytes.toString(x._3)
        val target_name = Bytes.toString(x._4)
        val relation = Bytes.toString(x._5)
        val relation_object = Bytes.toString(x._6)
        val weight = Bytes.toDouble(x._7)
        val create_time = Bytes.toString(x._8)
        val update_time = Bytes.toString(x._9)
        col_schema(source_id, source_name,target_id,target_name,relation, relation_object, weight,create_time,update_time)
      }
      }

    val ds1 = spark.createDataFrame(hbaseRDD)

    val url2 = "jdbc:mysql://10.20.7.156:3306/rck?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "rcDsj_56")

    //将结果保存到数据框中
    ds1.write.mode("append").jdbc(url2, "relation_new", prop2) //overwrite



    sc.stop()
    spark.stop()

  }

}
