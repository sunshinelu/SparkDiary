package com.evayInfo.Inglory.SparkDiary.mllib.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/10/24.
 * 参考链接：
 * 基本的统计工具（1） - spark.mllib
 * http://mocom.xmu.edu.cn/article/show/58482e8be083c990247075a8/0/1
 */
object sampleByKeyDemo1 {

  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    val SparkConf = new SparkConf().setAppName(s"sampleByKeyDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val data = sc.makeRDD(Array(
      ("female","Lily"),
      ("female","Lucy"),
      ("female","Emily"),
      ("female","Kate"),
      ("female","Alice"),
      ("male","Tom"),
      ("male","Roy"),
      ("male","David"),
      ("male","Frank"),
      ("male","Jack")))

    val fractions : Map[String, Double]= Map("female"->0.6,"male"->0.4)

    /*
    sampleByKey 方法
     */
    val approxSample = data.sampleByKey(withReplacement = false, fractions, 1)
    println("sampleByKey的结果为：")
    approxSample.collect().foreach(println)
    println("=====================")

    /*
    sampleByKeyExact 方法
     */

    val exactSample = data.sampleByKeyExact(withReplacement = false, fractions, 1)
    exactSample.collect().foreach(println)

    sc.stop()
    spark.stop()
  }
}
