package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/10/12.
 * 设置保留有效数字的位数
 */
object LimitDigit {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val SparkConf = new SparkConf().setAppName(s"LimitDigit").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val df = sc.parallelize(Seq(
      (1L, 0.58989), (2L, 10.2899809), (3L, 5.7937), (4L, -11.02374), (5L, 22.372349273)
    )).toDF("k", "v")
    df.show(false)
    val df1 = df.withColumn("v", bround($"v", 3))
    df1.show(false)




    sc.stop()
    spark.stop()
  }

}
