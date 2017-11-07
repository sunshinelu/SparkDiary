package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/11/7.
 */
object OneHotEncoderDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"OneHotEncoderDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")
    df.show(false)
    /*
+---+--------+
|id |category|
+---+--------+
|0  |a       |
|1  |b       |
|2  |c       |
|3  |a       |
|4  |a       |
|5  |c       |
+---+--------+
     */

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)
    indexed.show(false)
    /*
+---+--------+-------------+
|id |category|categoryIndex|
+---+--------+-------------+
|0  |a       |0.0          |
|1  |b       |2.0          |
|2  |c       |1.0          |
|3  |a       |0.0          |
|4  |a       |0.0          |
|5  |c       |1.0          |
+---+--------+-------------+
     */

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")

    val encoded = encoder.transform(indexed)
    encoded.show(false)
    /*

+---+--------+-------------+-------------+
|id |category|categoryIndex|categoryVec  |
+---+--------+-------------+-------------+
|0  |a       |0.0          |(2,[0],[1.0])|
|1  |b       |2.0          |(2,[],[])    |
|2  |c       |1.0          |(2,[1],[1.0])|
|3  |a       |0.0          |(2,[0],[1.0])|
|4  |a       |0.0          |(2,[0],[1.0])|
|5  |c       |1.0          |(2,[1],[1.0])|
+---+--------+-------------+-------------+
     */

    sc.stop()
    spark.stop()
  }


}
