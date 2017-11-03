package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/11/3.
 * 参考链接：
 * Spark Window Functions - rangeBetween dates：https://stackoverflow.com/questions/33207164/spark-window-functions-rangebetween-dates
 *
 *
 */
object WindowFuncDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"WindowFuncDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val colName = Array("id", "start", "some_value")
    val df = sc.parallelize(Seq(
    (1, "2015-01-01", 20.0),
    (1, "2015-01-06", 10.0),
    (1, "2015-01-07", 25.0),
    (1, "2015-01-12", 30.0),
    (2, "2015-01-01", 5.0),
    (2, "2015-01-03", 30.0),
    (2, "2015-02-01", 20.0)
    )).toDF(colName: _*).withColumn("start", col("start").cast("date")).
      withColumn("t", col("start").cast("timestamp").cast("long"))
    df.show(false)

//    days = lambda i: i * 86400

    val daysUdf = udf((i:Int) => (i * 86400))

    def daysFunc(i:Int) = {
      i * 86400
    }
    val w = Window.partitionBy("id")
      .orderBy(col("start").cast("timestamp").cast("long"))
//      .rangeBetween(-daysFunc(7), 0)
//      .rangeBetween(0, daysFunc(7))
      .rangeBetween(0, 1)

    df.select(col("*"), mean("some_value").over(w).alias("mean")).show(false)

    println(-daysFunc(7))
    println((20 + 10 + 25 + 30)/ 4)

    sc.stop()
    spark.stop()

  }
}
