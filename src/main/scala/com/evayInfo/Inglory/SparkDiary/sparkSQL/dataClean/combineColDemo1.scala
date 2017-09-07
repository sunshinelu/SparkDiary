package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/9/7.
 */
object combineColDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"combineColDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(
      (0, "cat26", 30.9), (0, "cat13", 22.1), (0, "cat95", 19.6), (0, "cat105", 1.3),
      (1, "cat67", 28.5), (1, "cat4", 26.8), (1, "cat13", 12.6), (1, "cat23", 5.3),
      (2, "cat56", 39.6), (2, "cat40", 29.7), (2, "cat187", 27.9), (2, "cat68", 9.8),
      (3, "cat8", 35.6))).toDF("Hour", "Category", "TotalValue")



    // 该方法不可行
    val df1 = df.withColumn("combCol", $"Category" + ":" + $"TotalValue")
    df1.show

    // 该方法可行
    val combinUDF = udf((col1: String, col2: Double) => col1 + ":" + col2.toString)
    val df2 = df.withColumn("combCol", combinUDF($"Category", $"TotalValue"))
    df2.show

    //    val pastUDF = udf((col1:String, col2:String) => col1 + ";"+ col2)
    //    val df3 = df2.groupBy("Hour").agg(pastUDF($"combCol", $"combCol"))
    //    df3.show

    val df4 = df.withColumn("r", round($"TotalValue", 3)).withColumn("b", bround($"TotalValue", 3))
    df4.show()

    val df5 = df2.groupBy("Hour").agg(sum($"combCol"))
    df5.show

    sc.stop()
    spark.stop()
  }

}
