package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/11/3.
 * 参考链接
 * Spark Window Functions for DataFrames and SQL：http://xinhstechblog.blogspot.com/2016/04/spark-window-functions-for-dataframes.html
 *
 */
object WindowFuncDemo2 {
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

    val customers = sc.parallelize(List(("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))).
      toDF("name", "date", "amountSpent")
    customers.show(false)
    /*
+-----+----------+-----------+
|name |date      |amountSpent|
+-----+----------+-----------+
|Alice|2016-05-01|50.0       |
|Alice|2016-05-03|45.0       |
|Alice|2016-05-04|55.0       |
|Bob  |2016-05-01|25.0       |
|Bob  |2016-05-04|29.0       |
|Bob  |2016-05-06|27.0       |
+-----+----------+-----------+
     */

    // Create a window spec.
    val wSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)
    // Calculate the moving average
    customers.withColumn("movingAvg",
      avg(customers("amountSpent")).over(wSpec1)).show(false)
    /*
+-----+----------+-----------+---------+
|name |date      |amountSpent|movingAvg|
+-----+----------+-----------+---------+
|Bob  |2016-05-01|25.0       |27.0     |
|Bob  |2016-05-04|29.0       |27.0     |
|Bob  |2016-05-06|27.0       |28.0     |
|Alice|2016-05-01|50.0       |47.5     |
|Alice|2016-05-03|45.0       |50.0     |
|Alice|2016-05-04|55.0       |50.0     |
+-----+----------+-----------+---------+
     */
    // Create a window spec.
    val wSpec1_temp = Window.partitionBy("name").orderBy("date").rowsBetween(0, 1)
    // Calculate the moving average
    customers.withColumn("movingAvg",
      avg(customers("amountSpent")).over(wSpec1_temp)).show(false)
    /*
+-----+----------+-----------+---------+
|name |date      |amountSpent|movingAvg|
+-----+----------+-----------+---------+
|Bob  |2016-05-01|25.0       |27.0     |
|Bob  |2016-05-04|29.0       |28.0     |
|Bob  |2016-05-06|27.0       |27.0     |
|Alice|2016-05-01|50.0       |47.5     |
|Alice|2016-05-03|45.0       |50.0     |
|Alice|2016-05-04|55.0       |55.0     |
+-----+----------+-----------+---------+
     */


    // Window spec: the frame ranges from the beginning (Long.MinValue) to the current row (0).
    val wSpec2 = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue, 0)

    // Create a new column which calculates the sum over the defined window frame.
    customers.withColumn( "cumSum",
      sum(customers("amountSpent")).over(wSpec2)).show(false)
    /*
+-----+----------+-----------+------+
|name |date      |amountSpent|cumSum|
+-----+----------+-----------+------+
|Bob  |2016-05-01|25.0       |25.0  |
|Bob  |2016-05-04|29.0       |54.0  |
|Bob  |2016-05-06|27.0       |81.0  |
|Alice|2016-05-01|50.0       |50.0  |
|Alice|2016-05-03|45.0       |95.0  |
|Alice|2016-05-04|55.0       |150.0 |
+-----+----------+-----------+------+
     */

    // Window spec. No need to specify a frame in this case.
    val wSpec3 = Window.partitionBy("name").orderBy("date")

    // Use the lag function to look backwards by one row.
    customers.withColumn("prevAmountSpent",
      lag(customers("amountSpent"), 1).over(wSpec3) ).show(false)
    /*

+-----+----------+-----------+---------------+
|name |date      |amountSpent|prevAmountSpent|
+-----+----------+-----------+---------------+
|Bob  |2016-05-01|25.0       |null           |
|Bob  |2016-05-04|29.0       |25.0           |
|Bob  |2016-05-06|27.0       |29.0           |
|Alice|2016-05-01|50.0       |null           |
|Alice|2016-05-03|45.0       |50.0           |
|Alice|2016-05-04|55.0       |45.0           |
+-----+----------+-----------+---------------+
     */

    // The rank function returns what we want.
    customers.withColumn( "rank", rank().over(wSpec3) ).show(false)
/*
+-----+----------+-----------+----+
|name |date      |amountSpent|rank|
+-----+----------+-----------+----+
|Bob  |2016-05-01|25.0       |1   |
|Bob  |2016-05-04|29.0       |2   |
|Bob  |2016-05-06|27.0       |3   |
|Alice|2016-05-01|50.0       |1   |
|Alice|2016-05-03|45.0       |2   |
|Alice|2016-05-04|55.0       |3   |
+-----+----------+-----------+----+
 */

    sc.stop()
    spark.stop()
  }

}
