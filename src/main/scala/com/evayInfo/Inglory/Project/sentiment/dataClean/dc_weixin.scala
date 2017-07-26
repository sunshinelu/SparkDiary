package com.evayInfo.Inglory.Project.sentiment.dataClean

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/7/19.
 *
 * 1. 微信数据表结构：

`DA_WEIXIN`：
  `WX_ID`：文章唯一标识
  `WX_KEY`：加密后的ID
  `WX_URL`：微信文章地址
  `WX_TITLE`：微信文章标题
  `WX_DATE`：微信文章时间
  `WX_CONTENT`：微信文章内容
  `WX_USER`
  `WX_TASK`：微信采集id
  `WX_IMG`
  `WX_ZT`：主题
  `CREATE_TIME`
  `DEL_FLAG`
 *
 *
3) `DA_WEIXIN`中获取数据为：
  `WX_ID`（文章唯一标识）
   `WX_TITLE`（微信文章标题）
  `WX_DATE`（微信文章时间）
  `WX_CONTENT`（微信文章内容）
  `WX_APPC`：带样式的内容
  `WX_ZT`（主题）
  `WX_URL`：微信文章地址
   新增一列`SOURCE`（来源）列：来源为`WEIXIN`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
 */
object dc_weixin {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"dc_weixin").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // get data from mysql database
    val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, "DA_WEIXIN").
      select("WX_ID", "WX_TITLE", "WX_CONTENT", "WX_APPC", "WX_ZT", "WX_DATE", "WX_URL")

    // add source column and IS_COMMENT column
    val addSource = udf((arg: String) => "WEIXIIN")
    val df2 = df1.withColumn("SOURCE", addSource($"WX_ID")).withColumn("IS_COMMENT", lit(0)).withColumn("glArticleId", lit(null))

    // change all columns name
    val colRenamed = Seq("articleId", "glArticleId", "title", "content", "keyword", "time", "is_comment", "source", "sourceUrl", "contentPre")
    val df3 = df2.select("WX_ID", "glArticleId", "WX_TITLE", "WX_APPC", "WX_ZT", "WX_DATE", "IS_COMMENT", "SOURCE", "WX_URL", "WX_CONTENT").
      toDF(colRenamed: _*).na.drop(Array("contentPre")).
      filter(length(col("contentPre")) >= 1)
    df3.printSchema()

    /*
    root
     |-- ARTICLEID: string (nullable = false)
     |-- TITLE: string (nullable = true)
     |-- TEXT: string (nullable = true)
     |-- KEYWORD: string (nullable = true)
     |-- TIME: string (nullable = true)
     |-- SOURCE: string (nullable = true)
     |-- IS_COMMENT: integer (nullable = false)
     |-- CONTENT: string (nullable = true)
     */


    // save data to database

    //    mysqlUtil.saveMysqlData(df3, url, user, password, "DC_WEIXIN", "overwrite") // save mode: overwrite OR append


    sc.stop()
    spark.stop()


  }

  /*
getWeixinData：获取清洗后的微信数据
*/
  def getWeixinData(spark: SparkSession, url: String, user: String, password: String,
                    wTable: String): DataFrame = {

    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, wTable).
      select("WX_ID", "WX_TITLE", "WX_CONTENT", "WX_APPC", "WX_ZT", "WX_DATE", "WX_URL")

    // add source column and IS_COMMENT column
    val addSource = udf((arg: String) => "WEIXIIN")
    val df2 = df1.withColumn("SOURCE", addSource(col("WX_ID"))).withColumn("IS_COMMENT", lit(0)).withColumn("glArticleId", lit(null))

    // change all columns name
    val colRenamed = Seq("articleId", "glArticleId", "title", "content", "keyword", "time", "is_comment", "source", "sourceUrl", "contentPre")
    val df3 = df2.select("WX_ID", "glArticleId", "WX_TITLE", "WX_APPC", "WX_ZT", "WX_DATE", "IS_COMMENT", "SOURCE", "WX_URL", "WX_CONTENT").
      toDF(colRenamed: _*).na.drop(Array("contentPre")).
      filter(length(col("contentPre")) >= 1)
    df3
  }



}
