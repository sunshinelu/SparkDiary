package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/10/16.
 */
object LogCalculateDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    // bulid spark environment
    val sparkConf = new SparkConf().setAppName(s"LogCalculateDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val df1ColumnsName = Seq("id", "weight")
    val df1 = sc.parallelize(Seq((1, 92834), (2, 9), (3, 97), (4, 822))).toDF(df1ColumnsName: _*)
    df1.show()
    /*
+---+------+
| id|weight|
+---+------+
|  1| 92834|
|  2|     9|
|  3|    97|
|  4|   822|
+---+------+
     */

    val df2 = df1.withColumn("log", bround(log($"weight"), 4))
    df2.show(false)

    /*
+---+------+-------+
|id |weight|log    |
+---+------+-------+
|1  |92834 |11.4386|
|2  |9     |2.1972 |
|3  |97    |4.5747 |
|4  |822   |6.7117 |
+---+------+-------+
     */
    sc.stop()
    spark.stop()
  }

}
