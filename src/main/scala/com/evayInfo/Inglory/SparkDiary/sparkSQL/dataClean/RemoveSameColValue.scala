package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/20.
 * 删除spark dataframe 中两列数据相同的行
 */
object RemoveSameColValue {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"RemoveSameColValue").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val df = sc.parallelize(Seq(
      (0, "cat26", "cat26", 30.9), (0, "cat13", "cat26", 22.1), (0, "cat95", "cat26", 19.6), (0, "cat105", "cat105", 1.3),
      (1, "cat67", "cat67", 28.5), (1, "cat4", "cat67", 26.8), (1, "cat13", "cat67", 12.6), (1, "cat23", "cat67", 5.3),
      (2, "cat56", "cat8", 39.6), (2, "cat40", "cat8", 29.7), (2, "cat187", "cat8", 27.9), (2, "cat68", "cat8", 9.8),
      (3, "cat8", "cat8", 35.6))).toDF("Hour", "Category", "Category2", "TotalValue")
    df.show(false)

    df.filter($"Category" =!= $"Category2").show(false)

    df.filter($"Category" === $"Category2").show(false)


    sc.stop()
    spark.stop()

  }

}
