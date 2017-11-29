package com.evayInfo.Inglory.ScalaDiary.Visualization.vegas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import vegas._
import vegas.sparkExt._

/**
 * Created by sunlu on 17/11/29.
 * https://github.com/vegas-viz/Vegas
 */
object vegasDemo2 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"vegasDemo2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = spark.createDataFrame(Seq(
      ("oneDayRetention", 0.0021),("threeDayRetention",0.0043),
      ("sevenDayRetention", 0.0043), ("fifthDayRetention",0.0043 )
    )).toDF("time","value")
    val plot = Vegas("Country Pop").withDataFrame(df1).
      encodeX("time", Nom).
      encodeY("value", Quant).
      mark(Bar)

    plot.show




    /*
    +-------------------+------+
|time               |value |
+-------------------+------+
|oneDayRetention    |0.0021|
|  ||
|  ||
|  ||
|oneMonthRetention  |0.0086|
|twoMonthRetention  |0.0107|
|threeMonthRetention|0.0107|
|fourMonthRetention |0.0043|
|fiveMonthRetention |0.0   |
|sixMonthRetention  |0.0   |
+-------------------+------+
     */
    sc.stop()
    spark.stop()
  }
}
