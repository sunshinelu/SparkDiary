package com.evayInfo.Inglory.SparkDiary.files

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/10/27.
 */
object readFilesDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"readFilesDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

//    val filepath = "file:///Users/sunlu/Workspaces/DataSets/NLP/data"
    val filepath = "file:///root/lulu/Progect/Test/data"
    val cidian = sc.textFile(filepath)
    val cidianDF = cidian.toDF("word")

    def regReplace(arg: String): String = {
      val enReplace = arg.replaceAll("[a-z]", "").replace(" ", "")
      val result = new String(enReplace.getBytes(), "utf-8")
      result
    }
    val regReplaceUdf = udf((arg: String) => regReplace(arg))
    val cidianDF_udf = cidianDF.withColumn("words2", regReplaceUdf($"word")).select("words2").dropDuplicates()
    cidianDF_udf.show(5, false)
//    cidianDF_udf.coalesce(1).write.mode(SaveMode.Overwrite).
//      text("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/dic")

    cidianDF_udf.coalesce(1).write.mode(SaveMode.Overwrite).
      text("file:///root/lulu/Progect/Test/dic1027")


    sc.stop()
    spark.stop()


  }


}
