package com.evayInfo.Inglory.Project.TextSummarization.DataClean

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup

/**
 * Created by sunlu on 18/1/30.
 */
object weibo {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"Data_Clean_Wibo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val url = "jdbc:mysql://localhost:3306/text_summarization?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"

    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, "DA_WEIBO").filter(col("TEXT").contains("【"))

    //使用Jsoup进行字符串处理
    val replaceString = udf((content: String) => {
      Jsoup.parse(content).body().text()
    })

    // 使用Jsoup进行字符串处理，并删除情感字符
    val emoticonPatten = "\\[[0-9a-zA-Z\\u4e00-\\u9fa5]+\\]".r
    val rmEmtionFunc = udf((arg: String) => {
      emoticonPatten.replaceAllIn(Jsoup.parse(arg).body().text(), "").mkString("")
    })
    val df2 = df1.withColumn("Jcontent", replaceString(col("TEXT")))
    val df3 = df1.withColumn("Jcontent", rmEmtionFunc(col("TEXT")))

    mysqlUtil.saveMysqlData(df3, url, user, password, "test1", "overwrite")
    

    sc.stop()
    spark.stop()
  }

}
