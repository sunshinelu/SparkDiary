package com.evayInfo.Inglory.Project.RenCai.PerformanceTest

import java.util.Properties
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*

spark-shell --master yarn --num-executors 4 --executor-cores  4 --executor-memory 4g
 */

object CheckData {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]): Unit = {
    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"CheckData").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val url2 = "jdbc:mysql://10.20.7.156:3306/rck?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "rcDsj_56")

    val ds1 = spark.read.jdbc(url2, "relation", prop2).drop("id")

    val ds2 = ds1.filter($"target_id" === "502d58d0-ea9e-4521-8a31-06e5e7aa13aa")

    ds2.show(truncate = false)

    sc.stop()
    spark.stop()



  }
}
