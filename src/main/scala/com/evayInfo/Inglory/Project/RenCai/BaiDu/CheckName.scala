package com.evayInfo.Inglory.Project.RenCai.BaiDu

import java.util.Properties

import com.evayInfo.Inglory.Project.RenCai.BaiDu.LabelToTag.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object CheckName {



  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]): Unit = {

    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"LabelToTag").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    val ds1 = spark.read.jdbc(url1, "label_tag", prop1).select("final_name") //  ; final_name ; alias_name

    val trim_udf = udf((x:String) => x.trim)
    val ds2 = ds1.withColumn("final_name", trim_udf($"final_name")).distinct() //  ; final_name ; alias_name
    println(ds2.count())

    val s1 = ds2.rdd.map{case Row(x:String) => x.trim}.distinct().collect().mkString(";")
    println(s1)

    sc.stop()
    spark.stop()
  }
}
