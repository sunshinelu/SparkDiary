package com.evayInfo.Inglory.Project.sentiment.sentimentAnalysis

import java.util.UUID

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Created by sunlu on 18/2/1.
 * 讲t_yq_all中的数据保存到yq_content和yq_article表中
 *
 */
object T2 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"T2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

//    val url2 = "jdbc:mysql://192.168.37.18:3306/bbs?useUnicode=true&characterEncoding=UTF-8"

    val url2 = "jdbc:mysql://localhost:3306/sunlu?useUnicode=true&characterEncoding=UTF-8"

    val user2 = "root"
    val password2 = "root"

    val articleTable = "yq_article"
    val contentTable = "yq_content"
    val allTable = "t_yq_all"
    val articleTable_New = "yq_article_new"
    val contentTable_New = "yq_content_new"


    def uuidFunc():String={
      val uuid = UUID.randomUUID().toString().toLowerCase()
      uuid
    }
    val uuidUDF = udf(() => uuidFunc())

    val all_df = mysqlUtil.getMysqlData(spark, url2, user2, password2, allTable).drop("id")
    val article_df = mysqlUtil.getMysqlData(spark, url2, user2, password2, articleTable).drop("id")
    val content_df = mysqlUtil.getMysqlData(spark, url2, user2, password2, contentTable)

    val content2_df = all_df.select("articleId", "content").union(content_df)
    println("start save table " + contentTable_New)
    content2_df.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("dbtable", contentTable_New)
      .option("url", url2)
      .option("user", user2)
      .option("password", password2)
      .option("numPartitions", "5")
      .save()
    println("succed save table " + contentTable_New)


    val article2_df = all_df.drop("content").union(article_df).withColumn("id",uuidUDF())
    //.dropDuplicates(Array("articleId"))

    println("start save table " + articleTable_New)
    article2_df.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("dbtable", articleTable_New)
      .option("url", url2)
      .option("user", user2)
      .option("password", password2)
      .option("numPartitions", "5")
      .save()
    println("succed save table " + articleTable_New)



    sc.stop()
    spark.stop()

  }
}
