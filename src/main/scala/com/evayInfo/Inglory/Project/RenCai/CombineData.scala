package com.evayInfo.Inglory.Project.RenCai

import java.util.Properties
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
spark-shell --master yarn --num-executors 8 --executor-cores  8 --executor-memory 8g --conf spark.yarn.executor.memoryOverhead=512 --conf spark.yarn.driver.memoryOverhead=512


spark-shell --master yarn --num-executors 8 --executor-cores  8 --executor-memory 8g --conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=1024M" --driver-java-options -XX:MaxPermSize=1024m

--conf spark.yarn.executor.memoryOverhead=4096

 */
object CombineData {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]): Unit = {
    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"CombineData").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val url2 = "jdbc:mysql://10.20.7.156:3306/rck?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "rcDsj_56")

    val ds1_xiaoyou = spark.read.jdbc(url2, "relation_xiaoyou", prop2).drop("id").filter($"weight" > 0)
    val ds1_tongshi = spark.read.jdbc(url2, "relation_tongshi", prop2).drop("id")

    //将结果保存到数据框中
    ds1_xiaoyou.coalesce(10).write.mode("append").jdbc(url2, "relation", prop2) //overwrite
    ds1_tongshi.coalesce(10).write.mode("append").jdbc(url2, "relation", prop2) //overwrite

    sc.stop()
    spark.stop()
  }
}
