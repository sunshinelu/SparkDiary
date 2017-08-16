package com.evayInfo.Inglory.Project.sentiment.sentimentAnalysis

import com.evayInfo.Inglory.Project.sentiment.dataClean._
import com.evayInfo.Inglory.util.mysqlUtil
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 * Created by sunlu on 17/8/3.
 * * 获取微博、微信、论坛贴吧、博客、搜索引擎、网站门户
 *
 *
 * 结果保存在SUMMARYARTICLE表中：
 * `SUMMARYARTICLE`
 * `ARTICLEID`：文章id
 * `TITLE`：文章标题
 * `CONTENT`：文章内容
 * `SOURCE`：文章来源
 * `KEYWORD`：标签:台湾  扶贫
 * `SCORE` ：文章得分
 * `LABEL`：标签：正类、负类、中性、严重
 * `TIME`：文章发表时间
 * `SYSTIME`：分析时间
 * `IS_COMMENT`：是否是评论 0：否 1：是
 */
object sentimentTrendV1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

     SetLogger

    val conf = new SparkConf().setAppName(s"sentimentTrendV1") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //    val url = "jdbc:mysql://localhost:3306/bbs"
    val url1 = "jdbc:mysql://192.168.37.18:3306/IngloryBDP?useUnicode=true&characterEncoding=UTF-8"
    //    val url1 = "jdbc:mysql://192.168.37.18:3306/IngloryBDP?useUnicode=true&characterEncoding=UTF-8&" +
    //      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
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


    //    val url2 = "jdbc:mysql://192.168.37.18:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
    //      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val url2 = "jdbc:mysql://192.168.37.18:3306/bbs?useUnicode=true&characterEncoding=UTF-8"
    val user2 = "root"
    val password2 = "root"
    val masterTable = "t_yq_article"
    val slaveTable = "t_yq_content"
    val allTable = "t_yq_all"

    val df_weibo = dc_weibo.getWeiboData(spark, url1, user1, password1, weiboAtable, weiboCtable)
    val df_weixin = dc_weixin.getWeixinData(spark, url1, user1, password1, weixinTable)
    val df_luntan = dc_luntan.getLuntanData(spark, url1, user1, password1, luntanAtable, luntanCtable)
    val df_search = dc_search.getSearchData(spark, url1, user1, password1, searchTable)
    val df_menhu = dc_menhu.getMenhuData(spark, url1, user1, password1, menhuTable)
    val df_bolg = dc_blog.getBlogData(spark, url1, user1, password1, blogTable)
    val id_df = mysqlUtil.getMysqlData(spark, url2, user2, password2, masterTable).select("ARTICLEID")

    val df = df_weibo.union(df_weixin).union(df_luntan).union(df_search).union(df_menhu).union(df_bolg).
      filter(length(col("time")) === 19) //.join(id_df, Seq("ARTICLEID"), "leftanti")

    //    val x = df.count()

    //    if (x > 0) {

    //获取正类、负类词典。posnegDF在join时使用；posnegList在词过滤时使用。
    val posnegDF = spark.read.format("CSV").option("header", "true").load("/personal/sunlu/Project/sentiment/posneg.csv")
    val posnegList = posnegDF.select("term").dropDuplicates().rdd.map { case Row(term: String) => term }.collect().toList

    //load stopwords file
    val stopwordsFile = "/personal/sunlu/Project/sentiment/Stopwords.dic"
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

    val df2 = df1.explode("segWords", "tokens") { segWords: String => segWords.split(" ") }
    //    df2.printSchema()

    val df3 = df2.join(posnegDF, df2("tokens") === posnegDF("term"), "left").na.drop()
    val df4 = df3.groupBy("articleId").agg(sum("weight")).withColumnRenamed("sum(weight)", "score")

    val df5 = df4.join(df, Seq("articleId"), "left").drop("contentPre").
      //      withColumn("IS_COMMENT", col("IS_COMMENT").cast("string")).
      withColumn("systime", current_timestamp()).withColumn("systime", date_format($"systime", "yyyy-MM-dd HH:mm:ss"))

    //      val mainDF = df5.na.drop(Array("title", "content")).drop("content")
    //      val slaveDF = df5.na.drop(Array("title", "content")).select("articleId", "content")

    val df6 = df5.filter(length(col("title")) >= 2).filter(length(col("content")) >= 2).dropDuplicates(Array("articleId"))
    val masterDF = df6.drop("content").dropDuplicates(Array("articleId"))
    val slaveDF = df6.select("articleId", "content").dropDuplicates(Array("articleId"))

    /*
    println("df6的数量为：" + df6.count)
    println("masterDF的数量为：" + masterDF.count)
    println("slaveDF的数量为：" + slaveDF.count)

    println("df6的分区数为：" + df6.rdd.partitions.size)
    println("masterDF的分区数为：" + masterDF.rdd.partitions.size)
    println("slaveDF的分区数为：" + slaveDF.rdd.partitions.size)

*/

    // truncate Mysql Table
    mysqlUtil.truncateMysql(url2, user2, password2, masterTable)
    mysqlUtil.truncateMysql(url2, user2, password2, slaveTable)
    mysqlUtil.truncateMysql(url2, user2, password2, allTable)
    // save Mysql Data
    //    mysqlUtil.saveMysqlData(slaveDF, url2, user2, password2, slaveTable, "append")
    //    mysqlUtil.saveMysqlData(masterDF, url2, user2, password2, masterTable, "append")
    //    mysqlUtil.saveMysqlData(df6, url2, user2, password2, allTable, "append")


    df6.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("dbtable", allTable)
      .option("url", url2)
      .option("user", user2)
      .option("password", password2)
      .option("numPartitions", 10)
      .save()


    masterDF.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("dbtable", masterTable)
      .option("url", url2)
      .option("user", user2)
      .option("password", password2)
      .option("numPartitions", 10)
      .save()

    slaveDF.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("dbtable", slaveTable)
      .option("url", url2)
      .option("user", user2)
      .option("password", password2)
      .option("numPartitions", 10)
      .save()


    //    }

    sc.stop()
    spark.stop()


  }
}
