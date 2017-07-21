package com.evayInfo.Inglory.Project.sentiment.dataClean

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/7/19.
 *
 * `DA_BAIDUARTICLE`
  `ID`：文章ID
  `CONTENT`：正文
  `TITLE`：标题
  `TIME`：时间
  `KEYWORD`：关键词
  `SOURCE`：源
  `TASKID`
  `SOURCEURL`：源url
  `CHARSET`：编码
 *
 *
 *
 * 改为：
 *
 * 6) `DA_BAIDUARTICLE`
  `ID`：文章ID
  `CONTENT`：正文
  `TITLE`：标题
  `TIME`：时间
  `KEYWORD`：关键词
   新增一列`SOURCE`（来源）列：来源为`SEARCH`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
 *
 */
object dc_search {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"dc_search").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // get data from mysql database
    val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, "DA_BAIDUARTICLE").
      select("ID", "TITLE", "CONTENT", "TIME", "KEYWORD")

    // add source column and IS_COMMENT column
    val df2 = df1.withColumn("SOURCE", lit("SEARCH")).withColumn("IS_COMMENT", lit(0))

    // change all columns name
    val colRenamed = Seq("ARTICLEID", "TITLE", "TEXT", "TIME", "KEYWORD", "SOURCE", "IS_COMMENT")
    val df3 = df2.toDF(colRenamed: _*).withColumn("CONTENT", $"TEXT").na.drop(Array("CONTENT"))
    df3.printSchema()
    /*
    root
 |-- ARTICLEID: string (nullable = true)
 |-- TITLE: string (nullable = true)
 |-- TEXT: string (nullable = true)
 |-- TIME: string (nullable = true)
 |-- KEYWORD: string (nullable = true)
 |-- SOURCE: string (nullable = false)
 |-- IS_COMMENT: integer (nullable = false)
 |-- CONTENT: string (nullable = true)
     */


  }

  /*
getSearchData：获取清洗后的搜索引擎数据
*/
  def getSearchData(spark: SparkSession, url: String, user: String, password: String,
                    TableName: String): DataFrame = {
    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, TableName).
      select("ID", "TITLE", "CONTENT", "TIME", "KEYWORD")

    // add source column and IS_COMMENT column
    val df2 = df1.withColumn("IS_COMMENT", lit(0)).withColumn("SOURCE", lit("SEARCH"))

    // change all columns name
    val colRenamed = Seq("ARTICLEID", "TITLE", "TEXT", "TIME", "KEYWORD", "IS_COMMENT", "SOURCE")
    val df3 = df2.toDF(colRenamed: _*).withColumn("CONTENT", col("TEXT")).na.drop(Array("CONTENT"))

    df3
  }

}
