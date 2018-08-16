package com.evayInfo.Inglory.SparkDiary.sparkSQL.functions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

/**
 * Created by sunlu on 18/8/16.
 * Adding ArrayType columns to Spark DataFrames with concat_ws and split
 * https://medium.com/@mrpowers/adding-arraytype-columns-to-spark-dataframes-with-concat-ws-and-split-cd18b5a929c1
 * https://github.com/MrPowers/spark-daria/blob/d4a91c828a334d590a216629e2200b9af8465d21/src/main/scala/com/github/mrpowers/spark/daria/sql/functions.scala
 */
object arrayExNullDemo1 {
  // do not print log
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"ScalaVectorToMlVector").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = Seq(
      ("i like blue and red"),
      ("you pink and blue")
    ).toDF("word1")

    /*
    提取字符串中的颜色，并保存成一个column，不存在的颜色用null表示
    方法一：
     */
    val actualDF1 = df.withColumn(
      "colors",
      array(
        when(col("word1").contains("blue"), "blue"),
        when(col("word1").contains("red"), "red"),
        when(col("word1").contains("pink"), "pink"),
        when(col("word1").contains("cyan"), "cyan")
      )
    )

    actualDF1.show(truncate = false)
    /*
+-------------------+------------------------+
|word1              |colors                  |
+-------------------+------------------------+
|i like blue and red|[blue, red, null, null] |
|you pink and blue  |[blue, null, pink, null]|
+-------------------+------------------------+
     */

    /*
提取字符串中的颜色，并保存成一个column，不存在的颜色用null表示
方法二：
 */
    val colors = Array("blue", "red", "pink", "cyan")

    val actualDF2 = df.withColumn(
      "colors",
      array(
        colors.map{ c: String =>
          when(col("word1").contains(c), c)
        }: _*
      )
    )
    actualDF2.show(truncate=false)
    /*
+-------------------+------------------------+
|word1              |colors                  |
+-------------------+------------------------+
|i like blue and red|[blue, red, null, null] |
|you pink and blue  |[blue, null, pink, null]|
+-------------------+------------------------+
 */

    /*
提取字符串中的字符串，并保存成一个column，不存在的字符串不保存
方法一：
 */
    val actualDF3 = df.withColumn(
      "colors",
      split(
        concat_ws(
          ",",
          when(col("word1").contains("blue"), "blue"),
          when(col("word1").contains("red"), "red"),
          when(col("word1").contains("pink"), "pink"),
          when(col("word1").contains("cyan"), "cyan")
        ),
        ","
      )
    )

    actualDF3.show(truncate=false)
    /*
    +-------------------+------------+
    |word1              |colors      |
    +-------------------+------------+
    |i like blue and red|[blue, red] |
    |you pink and blue  |[blue, pink]|
    +-------------------+------------+
     */

    /*
提取字符串中的字符串，并保存成一个column，不存在的字符串不保存
方法二：
 */
    def arrayExNull(cols: Column*): Column = {
      split(concat_ws(",,,", cols: _*), ",,,")
    }

    val actualDF = df.withColumn(
      "colors",
      arrayExNull(
        colors.map{ c: String =>
          when(col("word1").contains(c), c)
        }: _*
      )
    )

    actualDF.show(truncate=false)
/*
+-------------------+------------+
|word1              |colors      |
+-------------------+------------+
|i like blue and red|[blue, red] |
|you pink and blue  |[blue, pink]|
+-------------------+------------+
 */

    sc.stop()
    spark.stop()
  }

}
