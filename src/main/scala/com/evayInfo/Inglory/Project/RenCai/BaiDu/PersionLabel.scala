package com.evayInfo.Inglory.Project.RenCai.BaiDu


import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PersionLabel {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]): Unit = {


    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"PersionLabel").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    val ds1 = spark.read.jdbc(url1, "label_tag", prop1)
    ds1.printSchema()


    def trim_func(x:String):String={
      return x.trim
    }
    val trim_unf = udf((x:String) => trim_func(x))


    val ds2 = ds1.select("final_name").
      withColumn("final_name", trim_unf($"final_name")).dropDuplicates()
    println(ds2.count())
    ds2.show(truncate = false)
    /*
+----------+
|final_name|
+----------+
|著作译作      |
|工作经历      |
|研究方向      |
|学术兼职      |
|奖励情况      | 删除（删除原因：与"获奖情况"意思重复）
|教育经历      |
|科研项目      |
|获奖情况      |
|荣誉称号      |
|学术交流      |
+----------+


     */



    val ds3 = ds1.select("alias_name").withColumn("tag",lit(1)).
      groupBy($"alias_name").agg(sum($"tag")).orderBy($"sum(tag)".desc)
    println(ds3.count())
    ds3.show(truncate = false)

    sc.stop()
    spark.stop()
  }

}
