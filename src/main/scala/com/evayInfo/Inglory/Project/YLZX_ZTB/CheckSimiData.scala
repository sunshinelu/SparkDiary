package com.evayInfo.Inglory.Project.YLZX_ZTB

import java.util.Properties

import com.evayInfo.Inglory.Project.YLZX_ZTB.DocSimi_Title.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CheckSimiData {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    SetLogger

    //bulid environment
    val spark = SparkSession.builder.appName("DocSimi_Title").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://10.20.5.49:3306/efp5-ztb"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "BigData@2018")
    //get data
    val ds1 = spark.read.jdbc(url1, "ztb_tltle_simi", prop1)
    val ds2 = ds1.filter($"rn" === 1)
    println(ds2.count())

    val ds3 = ds2.filter($"shandong_title" === $"chine_title")
    println(ds3.count())

    sc.stop()
    spark.stop()
  }
}
