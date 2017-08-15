package com.evayInfo.Inglory.Project.sentiment.dataClean

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Created by sunlu on 17/7/19.
  *
  * `DA_BLOG`
  * `ID`：文章ID
  * `TITLE`：标题
  * `CONTENT`：正文
  * `URL`：链接地址
  * `CREATEDAT`：发表时间
  * `AUTHOR`：博主
  * `BLOG_KEY`：标签
  * `SYSTIME`：系统时间
  *
  * 修改为：
  *
  * 8) `DA_BLOG`
  * `ID`：文章ID
  * `TITLE`：标题
  * `CONTENT`：正文
  * `CONTENT_HTML`:带格式的正文
  * `CREATEDAT`：发表时间
  * `BLOG_KEY`：标签
  * `URL`：链接地址
  * 新增一列`SOURCE`（来源）列：来源为`BLOG`
  * 新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
  *
  *
  */
object dc_blog {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"dc_blog").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    // get data from mysql database
    val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"

    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, "DA_BLOG").
      select("ID", "TITLE", "CONTENT", "CONTENT_HTML", "CREATEDAT", "BLOG_KEY", "URL")

    // add source column and IS_COMMENT column
    val df2 = df1.withColumn("IS_COMMENT", lit(0)).withColumn("SOURCE", lit("BLOG")).withColumn("glArticleId", lit(null))

    // change all columns name
    val colRenamed = Seq("articleId", "glArticleId", "title", "content", "keyword", "time", "is_comment",
      "source", "sourceUrl", "contentPre")
    val df3 = df2.select("ID", "glArticleId", "TITLE", "CONTENT_HTML", "BLOG_KEY", "CREATEDAT", "IS_COMMENT",
      "SOURCE", "URL", "CONTENT").toDF(colRenamed: _*).na.drop(Array("contentPre")).
      filter(length(col("contentPre")) >= 1)
    df3.printSchema()
    /*
root
 |-- articleId: string (nullable = false)
 |-- glArticleId: null (nullable = true)
 |-- title: string (nullable = true)
 |-- content: string (nullable = true)
 |-- keyword: string (nullable = true)
 |-- time: string (nullable = true)
 |-- is_comment: integer (nullable = false)
 |-- source: string (nullable = false)
 |-- sourceUrl: string (nullable = true)
 |-- contentPre: string (nullable = true)
     */

    println("数据总数为：" + df3.count)
    println("除重后数据总数为：" + df3.dropDuplicates().count)
    println("articleId除重后数据总数为：" + df3.dropDuplicates(Array("articleId")).count)

  }

  /*
getBlogData：获取清洗后的门户网站数据
*/

  def getBlogData(spark: SparkSession, url: String, user: String, password: String,
                  TableName: String): DataFrame = {
    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, TableName).
      select("ID", "TITLE", "CONTENT", "CONTENT_HTML", "CREATEDAT", "BLOG_KEY", "URL")

    // add source column and IS_COMMENT column
    val df2 = df1.withColumn("IS_COMMENT", lit(0)).withColumn("SOURCE", lit("BLOG")).withColumn("glArticleId", lit(null))

    // change all columns name
    val colRenamed = Seq("articleId", "glArticleId", "title", "content", "keyword", "time", "is_comment",
      "source", "sourceUrl", "contentPre")
    val df3 = df2.select("ID", "glArticleId", "TITLE", "CONTENT_HTML", "BLOG_KEY", "CREATEDAT", "IS_COMMENT",
      "SOURCE", "URL", "CONTENT").toDF(colRenamed: _*).na.drop(Array("contentPre")).
      filter(length(col("contentPre")) >= 1)
    df3
  }

}
