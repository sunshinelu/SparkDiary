package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/9/6.
 */
object StringIndexerDemo {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"StringIndexerDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext


    val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.printSchema()
    /*
root
 |-- id: integer (nullable = false)
 |-- category: string (nullable = true)
 |-- categoryIndex: double (nullable = true)
     */
    indexed.show()


    sc.stop()
    spark.stop()

  }
}
