package com.evayInfo.Inglory.SparkDiary.files

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/11/12.
 */
object mysqlToHdfs {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"mysqlToHdfs").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/data_mining_db"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "Sogou_Classification_mini", prop1)

    val file_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/mysqlToHdfs"
    ds1.rdd.repartition(2).saveAsTextFile(file_path)

    val file_path_2 = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/mysqlToHdfs.txt"
    ds1.rdd.repartition(1).saveAsTextFile(file_path_2)

    sc.stop()
    spark.stop()

  }
}
