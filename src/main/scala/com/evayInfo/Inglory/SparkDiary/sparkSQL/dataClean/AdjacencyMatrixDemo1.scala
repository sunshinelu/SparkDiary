package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/11/1.
 * 参考链接：
 * http://kirillpavlov.com/blog/2016/02/21/top-5-features-released-in-spark-1.6/
 * https://databricks.com/blog/2016/02/09/reshaping-data-with-pivot-in-apache-spark.html
 */
object AdjacencyMatrixDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {


    SetLogger

    val sparkConf = new SparkConf().setAppName(s"AdjacencyMatrixDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Array(
      ("one", "A", 1), ("one", "B", 2), ("one", "C", 4),("two", "A", 5), ("two", "B", 6), ("two", "C", 7)
    )).toDF("key1", "key2", "value")

    df.show(false)

    df.groupBy("key1").pivot("key2").sum("value").show(false)


    val groupedData = df.groupBy("key1")

    groupedData.pivot("key2").sum("value").show(false)


    sc.stop()
    spark.stop()
  }

}
