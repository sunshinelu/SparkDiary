package com.evayInfo.Inglory.Project.sentiment.sentimentAnalysis

import com.evayInfo.Inglory.Project.sentiment.dataClean._
import com.evayInfo.Inglory.util.mysqlUtil
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/8/15.
 */
object T1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"sentimentTrendV1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val url1 = "jdbc:mysql://localhost:3306/bbs"
    val user1 = "root"
    val password1 = "root"

    val weiboAtable = "DA_WEIBO"
    val weiboCtable = "DA_WEIBO_COMMENTS"
    val weixinTable = "DA_WEIXIN"
    val luntanAtable = "DA_BBSARTICLE"
    val luntanCtable = "DA_BBSCOMMENT"
    val searchTable = "DA_BAIDUARTICLE"
    val menhuTable = "DA_SEED"
    val blogTable = "DA_BLOG"

    val masterTable = "t_yq_article"
    val slaveTable = "t_yq_content"

    val df_weibo = dc_weibo.getWeiboData(spark, url1, user1, password1, weiboAtable, weiboCtable)
    val df_weixin = dc_weixin.getWeixinData(spark, url1, user1, password1, weixinTable)
    val df_luntan = dc_luntan.getLuntanData(spark, url1, user1, password1, luntanAtable, luntanCtable)
    val df_search = dc_search.getSearchData(spark, url1, user1, password1, searchTable)
    val df_menhu = dc_menhu.getMenhuData(spark, url1, user1, password1, menhuTable)
    val df_bolg = dc_blog.getBlogData(spark, url1, user1, password1, blogTable)
    val id_df = mysqlUtil.getMysqlData(spark, url1, user1, password1, masterTable).select("ARTICLEID")

    //    val df = df_weixin.union(df_luntan).union(df_search).union(df_menhu).union(df_bolg).
    //      filter(length(col("time")) === 19).filter(length(col("title")) >= 2).filter(length(col("contentPre")) >= 2)
    //
    val df = df_weibo.union(df_weixin).union(df_luntan).union(df_search).union(df_menhu).union(df_bolg).
      filter(length(col("time")) === 19).filter(length(col("title")) >= 2) //.filter(length(col("contentPre")) >= 2)
    //      .join(id_df, Seq("ARTICLEID"), "leftanti").na.drop(Array("ARTICLEID")).dropDuplicates()
    println("df的数量为：" + df.count) //df的数量为：29106
    //    println("df除重后的数量为：" + df.dropDuplicates().count)//df除重后的数量为：29106
    println("df中ARTICLEID列除重后的数量为：" + df.dropDuplicates(Array("ARTICLEID")).count) //df中ARTICLEID列除重后的数量为：29106

    //     val df_1 =  df.filter(length(col("title")) >= 2).filter(length(col("content")) >= 2)
    //    println("df_1的数量为：" + df_1.count)//df_1的数量为：29106

    //获取正类、负类词典。posnegDF在join时使用；posnegList在词过滤时使用。
    val posnegDF = spark.read.format("CSV").option("header", "true").load("data/posneg.csv")
    val posnegList = posnegDF.select("term").dropDuplicates().rdd.map { case Row(term: String) => term }.collect().toList

    //load stopwords file
    val stopwordsFile = "data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //定义UDF
    //分词、停用词过滤、正类、负类词过滤
    val segWorsd = udf((content: String) => {
      ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word)).filter(word => posnegList.contains(word))
        .toSeq.mkString(" ")
    })

    val df1 = df.select("articleId", "contentPre").
      withColumn("segWords", segWorsd(column("contentPre")))

    println("df1的数量为：" + df1.count())

    sc.stop()
    spark.stop()
  }

}
