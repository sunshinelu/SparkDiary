package com.evayInfo.Inglory.SparkDiary.files

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by sunlu on 17/6/23.
  * 读取CSV文件
  * 参考链接：http://blog.csdn.net/u014046115/article/details/70142610
  * https://stackoverflow.com/questions/37271474/spark-scala-read-in-csv-file-as-dataframe-dataset
  */
object readCSV {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {
    //    Logger.getRootLogger.setLevel(Level.WARN)
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("VectorAssemblerDemo1").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    /*
    val ds1 = spark.read.csv("")
    spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", true).option("delimiter", ";").load("")

    val df = spark.read.option("header", true).csv("path")
*/

    val ColumnsName = Seq("words", "words2")
    val df1 = spark.read.option("header", true).option("delimiter", ",").
      csv("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/country.csv").toDF(ColumnsName: _*)
    df1.show()

    val df2 = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").
      option("header", true).option("delimiter", ",").
      load("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/country.csv").toDF("words", "words2")
    df2.show()


    sc.stop()
    spark.stop()

  }
}
