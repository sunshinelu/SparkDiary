package com.evayInfo.Inglory.Project.RenCai.DemoTest

import java.util.Properties

import com.evayInfo.Inglory.Project.RenCai.ImpactAnalysis.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ToLowerCase {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"ImpactAnalysis").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/talent"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    //get data
//    val ds1 = spark.read.jdbc(url1, "talent_info_new", prop1).select("talent_id", "name")
    val ds1 = spark.read.jdbc(url1, "pubication_info", prop1)

    val to_lower_udf = udf((x:String) => x.toLowerCase())
    val to_upper_udf = udf((x:String) => x.toUpperCase())

    val ds2 = ds1.withColumn("lower", to_lower_udf($"talent_id"))

    println(ds1.select("talent_id").distinct().count())
    println(ds2.select("lower").distinct().count())


    sc.stop()
    spark.stop()
  }

}
