package com.evayInfo.Inglory.SparkDiary.mllib.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/15.
 * 该函数类似于intersection，但返回在RDD中出现，并且不在otherRDD中出现的元素，不去重。参数含义同intersection
 * 参考链接：http://lxw1234.com/archives/2015/07/345.htm
 */
object subtractDemo {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"sentimentTrendV1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val rdd1 = sc.makeRDD(Seq(1, 2, 2, 3))
    println("rdd1的结果为：")
    rdd1.collect.foreach(println)

    val rdd2 = sc.makeRDD(3 to 4)
    println("rdd2的结果为：")
    rdd2.collect.foreach(println)

    println("rdd1减去rdd2的结果为：")
    rdd1.subtract(rdd2).collect.foreach(println)

    sc.stop()
    spark.stop()

  }
}
