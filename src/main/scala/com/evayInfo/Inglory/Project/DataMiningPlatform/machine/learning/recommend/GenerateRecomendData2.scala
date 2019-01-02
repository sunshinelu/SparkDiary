package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object GenerateRecomendData2 {
  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    // 链接mysql配置信息
    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)

    val SparkConf = new SparkConf().setAppName(s"GenerateRecomendData2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val col_name = Seq("user", "item","rating")
    val opt_path = "/Users/sunlu/Documents/Case/天枢大数据平台V3.0/数据挖掘分析平台/Evay人工智能开放平台测试数据/机器学习算法库/推荐/recommender_test.csv"
    val df1 = spark.read.
      option("header", true).
//      option("quote","\"").
      option("delimiter", ",").
      option("ignoreLeadingWhiteSpace", true).// If you have an extra space between your two data as "abc", "xyz", than you need to use
      csv(opt_path)toDF(col_name: _*)
    df1.show(truncate = false)
    df1.printSchema()

    val df2 = df1.withColumn("rating",$"rating".cast("double"))

    val opt_table = "recom_data"
    df2.write.mode(SaveMode.Overwrite).jdbc(url, opt_table,prop)




    sc.stop()
    spark.stop()
  }
}
