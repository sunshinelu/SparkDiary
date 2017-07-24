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
  `ID`：文章ID
  `TITLE`：标题
  `CONTENT`：正文
  `URL`：链接地址
  `CREATEDAT`：发表时间
  `AUTHOR`：博主
  `BLOG_KEY`：标签
  `SYSTIME`：系统时间
 *
 * 修改为：
 *
 * 8) `DA_BLOG`
  `ID`：文章ID
  `TITLE`：标题
  `CONTENT`：正文
  `CREATEDAT`：发表时间
  `BLOG_KEY`：标签
   新增一列`SOURCE`（来源）列：来源为`BLOG`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
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
      select("ID", "TITLE", "CONTENT", "CREATEDAT", "BLOG_KEY")

    // add source column and IS_COMMENT column
    val df2 = df1.withColumn("IS_COMMENT", lit(0)).withColumn("SOURCE", lit("BLOG"))

    // change all columns name
    val colRenamed = Seq("ARTICLEID", "TITLE", "TEXT", "TIME", "KEYWORD", "IS_COMMENT", "SOURCE")
    val df3 = df2.toDF(colRenamed: _*).withColumn("CONTENT", col("TEXT")).na.drop(Array("CONTENT"))
    df3.printSchema()
    /*
    root
 |-- ARTICLEID: string (nullable = false)
 |-- TITLE: string (nullable = true)
 |-- TEXT: string (nullable = true)
 |-- TIME: string (nullable = true)
 |-- KEYWORD: string (nullable = true)
 |-- IS_COMMENT: integer (nullable = false)
 |-- SOURCE: string (nullable = false)
 |-- CONTENT: string (nullable = true)
     */

  }

  /*
getBlogData：获取清洗后的门户网站数据
*/

  def getBlogData(spark: SparkSession, url: String, user: String, password: String,
                  TableName: String): DataFrame = {
    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, TableName).
      select("ID", "TITLE", "CONTENT", "CREATEDAT", "BLOG_KEY").filter(length(col("CONTENT")) >= 1)

    // add source column and IS_COMMENT column
    val df2 = df1.withColumn("IS_COMMENT", lit(0)).withColumn("SOURCE", lit("BLOG"))

    // change all columns name
    val colRenamed = Seq("ARTICLEID", "TITLE", "TEXT", "TIME", "KEYWORD", "IS_COMMENT", "SOURCE")
    val df3 = df2.toDF(colRenamed: _*).withColumn("CONTENT", col("TEXT")).na.drop(Array("CONTENT"))
    df3
  }

}
