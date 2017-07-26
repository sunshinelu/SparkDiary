package com.evayInfo.Inglory.SparkDiary.database.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Result
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
时间：2017年07月20日
在win下链接HBase，使用resource中的配置文件
参考链接：
http://blog.csdn.net/lanwenbing/article/details/40783335
https://github.com/srccodes/hadoop-common-2.2.0-bin

 */
object connHBaseWin {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {

    // 不显示日志
    //    SetLogger

    // 创建SparkContext
    val sparkConf = new SparkConf().setAppName("connHBaseWin").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //    System.setProperty("hadoop.home.dir", "D:\\Program Files\\Apache\\hadoop")

    val tableName = "t_hbaseFilter"
    // 设置hbase configure
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.37.21,192.168.37.22,192.168.37.23")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.master", "192.168.37.22:60000", "192.168.37.23:60000")

    hbaseConf.addResource("mapred-site.xml")
    hbaseConf.addResource("yarn-site.xml")
    hbaseConf.addResource("hbase-site.xml")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    // 创建hbase RDD
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    // 获取总行数
    val count = hBaseRDD.count()
    println("总行数为： " + count)

    sc.stop()
  }
}
