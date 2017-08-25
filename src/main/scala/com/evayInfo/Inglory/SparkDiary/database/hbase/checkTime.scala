package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Scan}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
  * Created by sunlu on 25/8/17.
  * 查看hbase时间
  *
  */
object checkTime {

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

  def main(args: Array[String]): Unit = {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"checkTime").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val outputTable = "t_check_time"

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    /*
    如果outputTable表存在，则删除表；如果不存在则新建表。
     */
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable)) {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    val htd = new HTableDescriptor(TableName.valueOf(outputTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名

    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    //定义时间格式
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime

    println("当前时间为：" + today)// 19位

    println("Long类型的当前时间为：" + todayL)

    val b_time = Bytes.toBytes(today)
    val b_timeL = Bytes.toBytes(todayL)
    val b_timeL_S = Bytes.toBytes(todayL.toString)

    println("String类型转Bytes为：" + b_time )
    println("Long类型转Bytes为：" + b_timeL )
    println("Long类型转String类型转Bytes为：" + b_timeL_S )

    val b_time_b_s = Bytes.toString(b_time)
    val b_time_b_l = Bytes.toLong(b_time)
    val b_timeL_b_l = Bytes.toLong(b_timeL)
    val b_timeL_b_s= Bytes.toString(b_timeL)
    val b_timeL_S_b_l =  Bytes.toLong(b_timeL_S)
    val b_timeL_S_b_s =  Bytes.toString(b_timeL_S)

    println("String类型转Bytes，Bytes转String为：" + b_time_b_s )

    println("String类型转Bytes，Bytes转Long为：" + b_time_b_l )

    println("Long类型转Bytes，Bytes转Long为：" + b_timeL_b_l )

    println("Long类型转Bytes，Bytes转String为：" + b_timeL_b_s)

    println("Long类型转String类型转Bytes，Bytes转Long为：" + b_timeL_S_b_l)

    println("Long类型转String类型转Bytes，Bytes转String为：" + b_timeL_S_b_s)

    sc.stop()
    spark.stop()
  }

}
