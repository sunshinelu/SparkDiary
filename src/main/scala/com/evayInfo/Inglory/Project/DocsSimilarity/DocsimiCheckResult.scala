package com.evayInfo.Inglory.Project.DocsSimilarity

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.evayInfo.Inglory.Project.DocsSimilarity.DocsimiUtil.{YlzxSchema, convertScanToString}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by sunlu on 17/8/26.
  */

object DocsimiCheckResult {
case class DocsimiSchema (id:String, simsID:String, level:String, title:String, manuallabel:String, websitename:String, mod:String)

  def getDocsimiRDD(docSimiTable: String, sc: SparkContext): RDD[DocsimiSchema] = {

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, docSimiTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("simsID")) //simsID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level")) //level
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("t")) //t
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //manuallabel
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //websitename
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mod")) //mod
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
      val title = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("t")) //t
      val manuallabel = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //manuallabel
      val websitename = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //websitename
      val mod = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("mod")) //mod
      (id, simsID, level, title, manuallabel, websitename, mod)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6 & null != x._7).
      map { x => {
        val id = Bytes.toString(x._1)
        val simsID =  Bytes.toString(x._2)
        val level = Bytes.toString(x._3)
        val title = Bytes.toString(x._4)
        val manuallabel = Bytes.toString(x._5)
        val websitename = Bytes.toString(x._6)
        val mod = Bytes.toString(x._7)
        DocsimiSchema(id, simsID, level, title, manuallabel, websitename, mod)
      }}

    hbaseRDD

  }


  def main(args: Array[String]): Unit = {

    DocsimiUtil.SetLogger
    val sparkConf = new SparkConf().setAppName(s"DocsimiCheckResult").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._



  }
}
