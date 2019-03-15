package com.evayInfo.Inglory.Project.RenCai.BaiDu


/*
select count(*) from cnki_details_dropdup_2
 */
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object DropDupUrl {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]): Unit = {

    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"DropDupUrl").setMaster("local[*]").
      set("spark.executor.memory", "6g").set("spark.driver.memory", "6g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    //connect mysql database
        val url1 = "jdbc:mysql://localhost:3306/cnki?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
//    val url1 = "jdbc:mysql://172.23.0.131:3316/rcdata?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    // 使用"useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"防止出现时间上的错误
    val prop1 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop1.setProperty("user", "root")
//    prop1.setProperty("password", "bigdata")
    prop1.setProperty("password", "root")

    val table_name = "cnki_details"
    //get data
    val ds1 = spark.read.jdbc(url1, table_name, prop1)
    //    println(ds1.count())

    val ds2 = ds1.dropDuplicates("url")//.select("title","content","url","keyword")
//    ds2.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(ds2.count())

    //    val ds3 = ds1.dropDuplicates(Array("title", "url"))
    //    println(ds3.count())


    ds2.drop("content").write.mode("overwrite").jdbc(url1, "cnki_details_dropdup_2", prop1) //overwrite;append


    sc.stop()
    spark.stop()
  }
}
