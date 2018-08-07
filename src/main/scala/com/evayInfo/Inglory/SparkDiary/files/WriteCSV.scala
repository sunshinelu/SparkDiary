package com.evayInfo.Inglory.SparkDiary.files

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by sunlu on 18/8/7.
 */
object WriteCSV {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("WriteCSV").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取csv文件，含表头
    val ColumnsName = Seq("words", "words2")
    val df1 = spark.read.option("header", true).option("delimiter", ",").
      csv("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/country.csv").toDF(ColumnsName: _*)
    df1.show()

    df1.select("id","features").coalesce(1).
      write.format("csv").option("delimiter", "#").mode(SaveMode.Append).
      save("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/csv_file")

    sc.stop()
    spark.stop()

  }

}
