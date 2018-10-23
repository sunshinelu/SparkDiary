package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/10/22.
 *
 * 生成构建推荐算法测试所需demo数据
 *
 */
object GenerateRecomendData {

  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {

    // 链接mysql配置信息
    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)

    val SparkConf = new SparkConf().setAppName(s"GenerateRecomendData").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val col_name = Seq("user", "item","rating")
    val opt_path = "/Users/sunlu/Documents/Case/天枢大数据平台V3.0/数据挖掘分析平台/数据挖掘平台所需demo数据/bigDataMiner_demodata/08推荐算法测试数据/Book-Crossing 书籍评分数据/BX-Book-Ratings.csv"
    val df1 = spark.read.
      option("header", true).
      option("quote","\"").
      option("delimiter", ";").
      option("ignoreLeadingWhiteSpace", true).// If you have an extra space between your two data as "abc", "xyz", than you need to use
      csv(opt_path)toDF(col_name: _*)
    df1.show(truncate = false)
    df1.printSchema()

    val N = df1.count()
    println("数据一共有 " + N + " 条！")

    val opt_table_name = "recommenderSys_Demo_Data"
    val opt_table_name_sample = "recommenderSys_Demo_Data_sample"

    df1.write.mode(SaveMode.Overwrite).jdbc(url, opt_table_name,prop)

    val fraction = 1000 / N.toDouble
    df1.sample(withReplacement = false,fraction = fraction , seed = 123L).
      write.mode(SaveMode.Overwrite).jdbc(url, opt_table_name_sample,prop)



    sc.stop()
    spark.stop()

  }

}
