package com.evayInfo.Inglory.Project.sentiment.dataClean

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Created by sunlu on 17/7/19.
  *
  * `DA_SEED`
  * `SEED_ID`：序号
  * `SEED_URL`：采集地址
  * `SEED_TITLE`：标题
  * `SEED_CONTENT`：内容
  * `SEED_DATE`：时间
  * `TASK_ID`：任务id
  * `CREATE_BY`：创建人
  * `CREATE_TIME`：创建时间
  * `UPDATE_BY`：修改人
  * `UPDATE_TIME`：修改时间
  * `DEL_FLAG`：删除标记 1:正常  2:删除
  * `MANUALLABEL`：标签
  * `TYPE`：区分网站、微信、微博
  * `FJFLAG`：标注是否为附件
  * `SOURCEURL`：源网页地址
  *
  *
  * 修改为：
  *
  * 7) `DA_SEED`
  * `SEED_ID`：序号
  * `SEED_TITLE`：标题
  * `SEED_CONTENT`：内容
  * `SEED_APPC`：带样式的内容
  * `SEED_DATE`：时间
  * `MANUALLABEL`：标签
  * `SOURCEURL`：源网页地址
  * 新增一列`SOURCE`（来源）列：来源为`MENHU`
  * 新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
  *
  *
  */
object dc_menhu {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"dc_menhu").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    // get data from mysql database
    val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, "DA_SEED").
      select("SEED_ID", "SEED_TITLE", "SEED_CONTENT", "SEED_APPC", "SEED_DATE", "MANUALLABEL", "SOURCEURL")

    // add source column and IS_COMMENT column
    val df2 = df1.withColumn("SOURCE", lit("MENHU")).withColumn("IS_COMMENT", lit(0)).withColumn("glArticleId", lit(null))

    df2.printSchema()
    // change all columns name
    val colRenamed = Seq("articleId", "glArticleId", "title", "content", "keyword", "time", "is_comment",
      "source", "sourceUrl", "contentPre")
    val df3 = df2.select("SEED_ID", "glArticleId", "SEED_TITLE", "SEED_APPC", "MANUALLABEL", "SEED_DATE",
      "IS_COMMENT", "SOURCE", "SOURCEURL", "SEED_CONTENT").
      toDF(colRenamed: _*) //.withColumn("contentPre", col("content")).na.drop(Array("content"))
    df3.printSchema()
    df3.select("source", "is_comment").show(3)


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
getMenhuData：获取清洗后的门户网站数据
*/

  def getMenhuData(spark: SparkSession, url: String, user: String, password: String,
                   TableName: String): DataFrame = {
    val df1 = mysqlUtil.getMysqlData(spark, url, user, password, TableName).
      select("SEED_ID", "SEED_TITLE", "SEED_CONTENT", "SEED_APPC", "SEED_DATE", "MANUALLABEL", "SOURCEURL")

    // add source column and IS_COMMENT column
    val df2 = df1.withColumn("SOURCE", lit("MENHU")).withColumn("IS_COMMENT", lit(0)).withColumn("glArticleId", lit(null))

    // change all columns name
    val colRenamed = Seq("articleId", "glArticleId", "title", "content", "keyword", "time", "is_comment",
      "source", "sourceUrl", "contentPre")
    val df3 = df2.select("SEED_ID", "glArticleId", "SEED_TITLE", "SEED_APPC", "MANUALLABEL", "SEED_DATE",
      "IS_COMMENT", "SOURCE", "SOURCEURL","SEED_CONTENT").
      toDF(colRenamed: _*)//.withColumn("contentPre", col("content")).na.drop(Array("content")).
      .filter(length(col("contentPre")) >= 1)
    df3
  }


}
