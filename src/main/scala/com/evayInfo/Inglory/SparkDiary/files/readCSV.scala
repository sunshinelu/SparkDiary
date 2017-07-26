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

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    //bulid environment
    val spark = SparkSession.builder.appName("VectorAssemblerDemo1").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val ds1 = spark.read.csv("")
    spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", true).option("delimiter", ";").load("")

    val df = spark.read.option("header", true).csv("path")

  }
}
