package com.evayInfo.Inglory.SparkDiary.files

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by sunlu on 17/10/23.
 */
object readFiles {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"readFiles") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val filepath = "file:///root/lulu/Progect/Test/data"

    val cidian = sc.textFile(filepath)
    cidian.take(5).foreach(println)

    val cidianDF = cidian.toDF("word")

    cidianDF.dropDuplicates().coalesce(1).write.mode(SaveMode.Overwrite).text("/personal/sunlu/ylzx/dic")

    cidianDF.dropDuplicates().filter($"word".contains("十九大")).show(false)

    sc.stop()
    spark.stop()
  }

}
