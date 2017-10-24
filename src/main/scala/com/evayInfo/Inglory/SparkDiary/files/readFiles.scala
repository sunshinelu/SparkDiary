package com.evayInfo.Inglory.SparkDiary.files

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
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


    val filepath2 = "file:///root/lulu/Progect/NLP/userDic_20171023.txt"
    val cidian2 = sc.textFile(filepath2)
    val cidianDF2 = cidian2.toDF("word")
    def regReplace(arg: String): String = {
      val result = arg.replaceAll("[a-z]", "").replace(" ", "")
      result
    }
    val regReplaceUdf = udf((arg: String) => regReplace(arg))

    val cidianDF_udf = cidianDF2.withColumn("words2", regReplaceUdf($"word")).select("words2").dropDuplicates()
    cidianDF_udf.filter($"words2".contains("者更")).show(false)
    cidianDF_udf.coalesce(1).write.mode(SaveMode.Overwrite).text("/personal/sunlu/ylzx/dic2")

    sc.stop()
    spark.stop()
  }

}
