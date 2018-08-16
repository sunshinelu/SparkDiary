package com.evayInfo.Inglory.SparkDiary.sparkSQL.functions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/8/16.
 *
 * Fuzzy Matching in Spark with Soundex and Levenshtein Distance
 * https://medium.com/@mrpowers/fuzzy-matching-in-spark-with-soundex-and-levenshtein-distance-6749f5af8f28
 */
object levenshteinDemo1 {

  // do not print log
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {

    SetLogger
    val conf = new SparkConf().setAppName(s"levenshteinDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val list1 = List(("to","two"),("brake","break"),("here","hear"),("tree","free"))
    val df1 = list1.toDF("word1","word2")
    df1.show(truncate = false)

    /*
    +-----+-----+
|word1|word2|
+-----+-----+
|   to|  two|
|brake|break|
| here| hear|
| tree| free|
+-----+-----+
     */

    /*
    how these words are encoded by the soundex algorithm.
     */
    val actualDF1 = df1.withColumn(
      "w1_soundex",
      soundex(col("word1"))
    ).withColumn(
      "w2_soundex",
      soundex(col("word2"))
    )

    actualDF1.show()
    /*
    +-----+-----+----------+----------+
|word1|word2|w1_soundex|w2_soundex|
+-----+-----+----------+----------+
|   to|  two|      T000|      T000|
|brake|break|      B620|      B620|
| here| hear|      H600|      H600|
| tree| free|      T600|      F600|
+-----+-----+----------+----------+
     */

    val actualDF2 = df1.withColumn(
      "name1_name2_soundex_equality",
      soundex(col("word1")) === soundex(col("word2"))
    )
    actualDF2.show()
    /*
    +-----+-----+----------------------------+
|word1|word2|name1_name2_soundex_equality|
+-----+-----+----------------------------+
|   to|  two|                        true|
|brake|break|                        true|
| here| hear|                        true|
| tree| free|                       false|
+-----+-----+----------------------------+
     */

    /*
    calculate the Levenshtein distance between word1 and word2.
     */

    val actualDF3 = df1.withColumn(
      "word1_word2_levenshtein",
      levenshtein(col("word1"), col("word2"))
    )

    actualDF3.show()

    sc.stop()
    spark.stop()
  }

}
