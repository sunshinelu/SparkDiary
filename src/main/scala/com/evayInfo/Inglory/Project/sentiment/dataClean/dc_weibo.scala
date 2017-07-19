package com.evayInfo.Inglory.Project.sentiment.dataClean

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup

/**
 * Created by sunlu on 17/7/19.
 */
object dc_weibo {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"dc_weibo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    //    val url = "jdbc:mysql://localhost:3306/bbs"
    val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"

    //    val df = new mysqlUtil()
    //    df.getMysqlData(spark, url, user, password, "DA_WEIBO")

    val df_w = mysqlUtil.getMysqlData(spark, url, user, password, "DA_WEIBO")
    val df_c = mysqlUtil.getMysqlData(spark, url, user, password, "DA_WEIBO_COMMENTS")






    //使用Jsoup进行字符串处理
    val replaceString = udf((content: String) => {
      Jsoup.parse(content).body().text()
    })

    // 使用Jsoup进行字符串处理，并删除情感字符
    val emoticonPatten = "\\[[0-9a-zA-Z\\u4e00-\\u9fa5]+\\]".r
    val rmEmtionFunc = udf((arg: String) => {
      emoticonPatten.replaceAllIn(Jsoup.parse(arg).body().text(), "").mkString("")
    })

    val df1 = df_w.withColumn("content", rmEmtionFunc(col("TEXT")))
    df1.select("content").take(15).foreach(println)

    val df2 = df_c.withColumn("content", replaceString(col("TEXT")))
    df2.select("content").take(15).foreach(println)



    // 获取"@用户名"

    val userPatten = "@[\\u4e00-\\u9fa5a-zA-Z0-9_-]{4,30}".r
    //val userPatten = "@[^,，：:\\s@]+".r//这种

    def getUsers(content: String): String = {
      val userNames = userPatten.findAllMatchIn(content).mkString(";")
      userNames
    }
    val getUserFunc2 = udf((arg: String) => getUsers(arg))

    val getUserFunc = udf((content: String) => {
      val userNames = userPatten.findAllMatchIn(content).mkString(";")
      userNames
    })

    // 获取"#话题#"
    val topicPatten = "#[^#]+#".r
    val getTopicsFunc = udf((content: String) => {
      val userNames = topicPatten.findAllMatchIn(content).mkString(";")
      userNames
    })

    val df1_get = df1.withColumn("topics", getTopicsFunc(col("content")))
    df1_get.select("topics").take(15).foreach(println)


  }

}
