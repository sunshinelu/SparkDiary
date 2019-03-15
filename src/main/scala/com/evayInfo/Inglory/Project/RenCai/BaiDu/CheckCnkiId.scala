package com.evayInfo.Inglory.Project.RenCai.BaiDu

import java.util.Properties

import com.evayInfo.Inglory.Project.RenCai.BaiDu.CheckName.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
  * @Author: sunlu
  * @Date: 2019-02-28 09:52
  * @Version 1.0
  */
object CheckCnkiId {

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
    val url1 = "jdbc:mysql://localhost:3306/cnki?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

//    val ds1 = spark.read.jdbc(url1, "baike", prop1).select("cnki_id").dropDuplicates()
//    println(ds1.count())

    val ds1_2 = spark.read.jdbc(url1, "baike", prop1).select("name").
      withColumn("tag",lit(1)).
      groupBy("name").agg(sum($"tag")).orderBy($"sum(tag)".desc)
    println(ds1_2.count())
    ds1_2.show(truncate = false)



//    val ds2 = spark.read.jdbc(url1,"cnki_details_dropdup_2",prop1).select("id").dropDuplicates()
//    println(ds2.count())


//    val ds3 = spark.read.jdbc(url1,"cnki_details_dropdup",prop1).filter(length($"content") >= 10).select("id").dropDuplicates()
//    println(ds3.count())

    val ds4 = spark.read.jdbc(url1,"basicinfo",prop1).select("name").
      withColumn("name",regexp_replace($"name"," ","")).
      withColumn("name",regexp_replace($"name","　","")).
      withColumn("name",regexp_replace($"name",":","")).
      withColumn("name",regexp_replace($"name","：","")).
      withColumn("tag",lit(1)).
      groupBy("name").agg(sum($"tag")).
      coalesce(1).orderBy($"sum(tag)".desc)
//    println(ds4.count())
//    ds4.show(truncate = false)

    ds4.write.mode("overwrite").jdbc(url1, "persion_tag", prop1) //overwrite;append



    sc.stop()
    spark.stop()
  }

}
