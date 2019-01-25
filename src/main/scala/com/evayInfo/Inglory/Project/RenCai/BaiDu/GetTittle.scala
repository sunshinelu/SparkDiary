package com.evayInfo.Inglory.Project.RenCai.BaiDu

import java.util.Properties

import com.evayInfo.Inglory.Project.RenCai.TongShiRelation.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetTittle {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]): Unit = {

    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"GetTittle").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //    val url2 = "jdbc:mysql://172.23.0.131:3316/rcdata?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val url = "jdbc:mysql://172.23.0.131:3316/rcdata?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    // 使用"useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"防止出现时间上的错误
    val prop = new Properties()
    prop.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop.setProperty("user", "root")
    prop.setProperty("password", "bigdata")

    //get data
    val ds1 = spark.read.jdbc(url, "cnki_details_title", prop).select("title").na.drop()
    val ds2 = ds1.withColumn("getTitle", explode(split($"title", ","))).
      withColumn("tag",lit(1)).na.drop()
    val trim_udf = udf((x:String) => x.trim)
    val ds3 = ds2.withColumn("getTitle",trim_udf($"getTitle")).filter(length($"getTitle") >=2).
      withColumn("getTitle",regexp_replace($"getTitle","：","")).
      groupBy("getTitle").agg(sum("tag")).orderBy($"sum(tag)".desc)

    ds3.show(truncate = false)



    sc.stop()
    spark.stop()
  }

}
