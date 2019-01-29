package com.evayInfo.Inglory.Project.RenCai.BaiDu

import java.util.Properties

import com.evayInfo.Inglory.Project.RenCai.PerformanceTest.XiaoYouRelationHBase.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object LabelToTag {


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

    val ds1_qsbdbk = spark.read.jdbc(url1, "qsbdbk", prop1).drop("id")
    val ds1_bdbkbt = spark.read.jdbc(url1, "bdbkbt", prop1).drop("id").
      withColumnRenamed("name","final_name")

    val ds2 = ds1_qsbdbk.join(ds1_bdbkbt,Seq("alias_name"),"outer").
      sort($"final_name").na.drop().dropDuplicates()

//    ds2.show(truncate = false)

    ds2.coalesce(10).write.mode("overwrite").jdbc(url1, "label_tag", prop1) //overwrite;append

    /*
    name -> alias_name -> final_name
    采集 -> 解析 -> 信息抽取
     */



    sc.stop()
    spark.stop()
  }
}
