package com.evayInfo.Inglory.Project.sentiment.dataClean

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
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
`DA_WEIXIN`中获取数据为：
  `WX_ID`（文章唯一标识）
   `WX_TITLE`（微信文章标题）
  `WX_DATE`（微信文章时间）
  `WX_CONTENT`（微信文章内容）
  `WX_ZT`（主题）
   新增一列`SOURCE`（来源）列：来源为`WEIXIN`
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
      select("WX_ID", "WX_TITLE", "WX_CONTENT", "WX_ZT", "WX_DATE")

    // add source column
    val addSource = udf((arg: String) => "WEIXIIN")
    val df2 = df1.withColumn("Source", addSource($"WX_ID"))
    df2.printSchema()

    // change all columns name
    val colRenamed = Seq("ID", "TITLE", "CONTENT", "TOPIC", "DATE", "SOURCE")
    val df3 = df2.toDF(colRenamed: _*)
    df3.printSchema()

    // save data to database

    mysqlUtil.saveMysqlData(df3, url, user, password, "DC_WEIXIN", "overwrite") // save mode: overwrite OR append


    sc.stop()
    spark.stop()


  }

}
