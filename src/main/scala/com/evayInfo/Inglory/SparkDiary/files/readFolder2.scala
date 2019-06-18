package com.evayInfo.Inglory.SparkDiary.files

import java.util.Properties

import com.evayInfo.Inglory.SparkDiary.files.readFolder.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Author: sunlu
  * @Date: 2019-06-18 15:45
  * @Version 1.0
  *
  *  批量读取csv文件
  */
object readFolder2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {

    SetLogger

    val SparkConf = new SparkConf().setAppName(s"readFolder").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val filePath = "/Users/sunlu/Downloads/Enterprise-Registration-Data-of-Chinese-Mainland-master 2/csv/*/*"

    val df1 = spark.read.option("header", true).option("delimiter", ",").
      csv(filePath).toDF()
//    df1.show()
//    println(df1.count()) // 5888641
//
//    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
//      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
//    val user = "root"
//    val password = "root"
//    val prop = new Properties()
//    prop.setProperty("user", user)
//    prop.setProperty("password", password)
//    val opt_table = "gongsi"
//
//    df1.write.mode(SaveMode.Append).jdbc(url, opt_table, prop)
    df1.coalesce(1).
      write.format("csv").mode(SaveMode.Overwrite).
      save("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/gongsi")



    sc.stop()
    spark.stop()
  }

}
