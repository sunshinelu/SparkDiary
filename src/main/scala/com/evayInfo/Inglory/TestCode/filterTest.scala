package com.evayInfo.Inglory.TestCode

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/25.
 */
object filterTest {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"filterTest").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val url1 = "jdbc:mysql://172.16.10.141:3306/sunhao?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver")
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "123456")
    //get data
    val ds1 = spark.read.jdbc(url1, "rc_jbxx", prop1)
    ds1.printSchema()
    val ds2 = ds1.filter($"source" === "长江学者")

    println("ds2 is: " + ds2.count())

    val ds3 = ds1.filter($"source".contains("长江学者"))
    println("ds3 is: " + ds3.count())

    sc.stop()
    spark.stop()
  }
}
