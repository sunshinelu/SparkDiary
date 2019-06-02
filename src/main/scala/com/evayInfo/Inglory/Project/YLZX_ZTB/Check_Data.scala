package com.evayInfo.Inglory.Project.YLZX_ZTB

import java.util.Properties

import com.evayInfo.Inglory.Project.YLZX_ZTB.DocSimi_Title.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Check_Data {
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
    val ds1 = spark.read.jdbc(url1, "collect_ccgp", prop1)


    val col_names = Seq("id","title", "data", "website").map(col(_))
    val ds2 = ds1.select(col_names: _*)

    val s = "青岛职业技术学院学前教育专业智慧实训室建设项目更正公告"
    val ds3 = ds2.filter($"title" === s)

    ds3.show(truncate = false)





    sc.stop()
    spark.stop()

  }
}
