package com.evayInfo.Inglory.Project.DataMiningPlatform.text.analysis

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by sunlu on 18/10/18.
 * 在 GenerateTestData 的基础上随机取100条数据
 */
object GenerateTestData2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"GenerateTestData2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    val ipt_table = "Sogou_Classification_mini_segWords"
//    val opt_table = "Sogou_Classification_mini_segWords_random100"
    val opt_table = "Sogou_Classification_mini_segWords_random10"




    val df = spark.read.jdbc(url, ipt_table, prop)
    val N = df.count().toDouble
//    val fraction = 100 / N
    val fraction = 10 / N
    val df1 = df.sample(withReplacement = false, fraction = fraction, seed = 123L)

    df1.write.mode(SaveMode.Overwrite).jdbc(url, opt_table, prop)

    sc.stop()
    spark.stop()
  }

}
