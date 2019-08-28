package com.evayInfo.Inglory.SparkDiary.sparkSQL.functions


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}

/**
  * @Author: sunlu
  * @Date: 2019-08-27 16:48
  * @Version 1.0
  *
  * 参考链接：
  * Working with Spark ArrayType columns
  * https://mungingdata.com/apache-spark/arraytype-columns/
  *
  */
object ArrayType_columns {

  // do not print log
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]): Unit = {
    SetLogger
    val conf = new SparkConf().setAppName(s"ArrayType_columns").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val peopleDF = Seq(
      ("bob", Array("red", "blue")),
      ("maria", Array("green", "red")),
      ("sue", Array("black"))
    ).toDF("name", "favorite_colors")

    peopleDF.show(truncate = false)
    peopleDF.printSchema()

    val actualDF = peopleDF.withColumn(
      "likes_red",
      array_contains(col("favorite_colors"), "red")
    )

    actualDF.show(truncate = false)
    actualDF.printSchema()


    val df = peopleDF.select(
      col("name"),
      explode(col("favorite_colors")).as("color")
    )
    df.show(truncate = false)




    sc.stop()
    spark.stop()
  }

}
