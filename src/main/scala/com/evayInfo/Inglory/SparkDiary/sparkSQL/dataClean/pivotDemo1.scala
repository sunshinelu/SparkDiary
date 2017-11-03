package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/11/2.
 * 参考链接：
 * Pivoting Data in SparkSQL: https://svds.com/pivoting-data-in-sparksql/
 *
 * 长宽表转换之长表转款表
 */
object pivotDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"pivotDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

//   val df = spark.read.format("csv").options(header='true', inferschema='true').load('mpg.csv')

    val df = spark.read.option("header", true).option("inferschema", true).option("delimiter", ",").
      csv("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/mpg.csv")
    df.show(5, false)

    val df1 = df.groupBy("class","year").agg(avg("hwy"))
    df1.show(5, false)

    val df2 = df.groupBy("class").pivot("year").agg(avg("hwy"))
    df2.show(5, false)

    val df3 = df.groupBy("class").pivot("year", Seq(1999, 2008)).agg(avg("hwy"))
    df3.show(5, false)

    val df4 =  df.groupBy("class").pivot("year", Seq(1999, 2008)).agg(min("hwy"), max("hwy"))
    df4.show(5, false)

    sc.stop()
    spark.stop()
  }

}
