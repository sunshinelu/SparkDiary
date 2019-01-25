package com.evayInfo.Inglory.Project.RenCai.DemoTest

import java.util.Properties

import com.evayInfo.Inglory.Project.RenCai.FamiliarityRelation.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object CheckRelationResult {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"FamiliarityRelation").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    /*
val url1 = "jdbc:mysql://10.20.7.156:3306/talent?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
val prop1 = new Properties()
prop1.setProperty("user", "root")
prop1.setProperty("password", "rcDsj_56")
 */

    val relation_ds1 = spark.read.jdbc(url1, "relation", prop1)
    val relation_ds2 = relation_ds1.select("source_name","target_name").
      filter($"source_name".contains("岳寿伟") || $"source_name".contains("张迪")).
      filter($"target_name".contains("岳寿伟") || $"target_name".contains("张迪"))
    relation_ds2.show(truncate = false)

    sc.stop()
    spark.stop()

  }
}
