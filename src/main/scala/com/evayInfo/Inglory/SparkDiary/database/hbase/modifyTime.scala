package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by sunlu on 25/8/17.
  * 修改yilan-total_webpage表中的时间
  *
  */

object modifyTime {
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

    val sparkConf = new SparkConf().setAppName(s"modifyTime").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val ylzxTable = "yilan-total_webpage"
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, ylzxTable) //设置输出表名
    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)


    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      (rowkey, time)
    }
    }.filter(x => null != x._2).
      map { x => {
        val rowkey = Bytes.toString(x._1)
        //时间格式转化
        val time = Bytes.toLong(x._2)
        (rowkey, time)
      }
      }.filter(_._2.toString.length == 19)

//      val hbaseDF = hbaseRDD.toDF("rowkey", "time").coalesce(1).write.mode(SaveMode.Overwrite).csv("file:///D:\\Workspace\\IDEA\\GitHub\\SparkDiary\\result\\yilan-total_webpage_Rowkey2")
        println("yilan-total_webpage表中时间列toLong的长度为19的数量为：" + hbaseRDD.count())
    //yilan-total_webpage表中时间列toLong的长度为19的数量为：66002
//yilan-total_webpage表中时间列toLong的长度为19的数量为：2307

    val hbaseRDD3 = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      (rowkey, time)
    }
    }.filter(x => null != x._2).
      map { x => {
        val rowkey = Bytes.toString(x._1)
        //时间格式转化
        val time = Bytes.toLong(x._2)
        (rowkey, time)
      }
      }.filter(_._2.toString.length == 13)
    println("修改后：yilan-total_webpage表中时间列toLong的长度为13的数量为：" + hbaseRDD3.count())
//    修改后：yilan-total_webpage表中时间列toLong的长度为13的数量为：125659
//修改后：yilan-total_webpage表中时间列toLong的长度为13的数量为：125659
    //修改后：yilan-total_webpage表中时间列toLong的长度为13的数量为：127966

    /*
    val rowkeyList = hbaseRDD.map(_._1).collect().toList

    val hbaseRDD2 = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      (rowkey, time)
    }
    }.filter(x => null != x._2).
      map { x => {
        val rowkey = Bytes.toString(x._1)
        //时间格式转化
        val time = Bytes.toString(x._2)
        (rowkey, time)
      }
      }
    //定义时间格式
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    val filterRDD = hbaseRDD2.filter(x => {
      rowkeyList.contains(x._1)
    }).map( x => {
      val rowkey = x._1
      val time = x._2 match {
        case t if (t.contains("-")) => dateFormat.parse(t).getTime
        case _ => x._2.toLong
      }
      (rowkey, time)
    })


//    filterRDD.take(5).foreach(println)
//    println("filterRDD的长度为：" + filterRDD.count())


    filterRDD.map(x => {
      val key = Bytes.toBytes(x._1)
      val put = new Put(key)
      put.add(Bytes.toBytes("f"), Bytes.toBytes("mod"), Bytes.toBytes(x._2.toLong))
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(jobConf)
*/

    /*
    count 'yilan-total_webpage'
    => 143597

     */




    sc.stop()
    spark.stop()
  }

}
