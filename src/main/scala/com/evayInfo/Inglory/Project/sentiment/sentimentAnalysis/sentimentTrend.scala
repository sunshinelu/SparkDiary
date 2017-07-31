package com.evayInfo.Inglory.Project.sentiment.sentimentAnalysis

import com.evayInfo.Inglory.Project.sentiment.dataClean._
import com.evayInfo.Inglory.util.mysqlUtil
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/7/19.
 * 获取微博、微信、论坛贴吧、博客、搜索引擎、网站门户
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
object sentimentTrend {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"sentimentTrend").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //    val url = "jdbc:mysql://localhost:3306/bbs"
    val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"

    val weiboAtable = "DA_WEIBO"
    val weiboCtable = "DA_WEIBO_COMMENTS"
    val weixinTable = "DA_WEIXIN"
    val luntanAtable = "DA_BBSARTICLE"
    val luntanCtable = "DA_BBSCOMMENT"
    val searchTable = "DA_BAIDUARTICLE"
    val menhuTable = "DA_SEED"
    val blogTable = "DA_BLOG"
    val masterTable = "yq_article"
    val slaveTable = "yq_content"

    val df_weibo = dc_weibo.getWeiboData(spark, url, user, password, weiboAtable, weiboCtable)
    val df_weixin = dc_weixin.getWeixinData(spark, url, user, password, weixinTable)
    val df_luntan = dc_luntan.getLuntanData(spark, url, user, password, luntanAtable, luntanCtable)
    val df_search = dc_search.getSearchData(spark, url, user, password, searchTable)
    val df_menhu = dc_menhu.getMenhuData(spark, url, user, password, menhuTable)
    val df_bolg = dc_blog.getBlogData(spark, url, user, password, blogTable)

    /*
    println("df_weibo")
    df_weibo.printSchema()
    println("df_weixin")
    df_weixin.printSchema()
    println("df_luntan")
    df_luntan.printSchema()
    println("df_search")
    df_search.printSchema()
    println("df_menhu")
    df_menhu.printSchema()
*/
    val df = df_weibo.union(df_weixin).union(df_luntan).union(df_search).union(df_menhu).union(df_bolg).
      filter(length(col("time")) === 19)

    //    println(df.select("IS_COMMENT").distinct().show())

    df.printSchema()
    /*
root
 |-- articleId: string (nullable = true)
 |-- glArticleId: string (nullable = true)
 |-- title: string (nullable = true)
 |-- content: string (nullable = true)
 |-- keyword: string (nullable = true)
 |-- time: string (nullable = true)
 |-- is_comment: integer (nullable = true)
 |-- source: string (nullable = true)
 |-- sourceUrl: string (nullable = true)
 |-- contentPre: string (nullable = true)
     */
    /*
    println("count weibo data: " + df_weibo.count()) // error ==> success
    println("count weixin data: " + df_weixin.count()) // success
    println("count luntan data: " + df_luntan.count()) // success
    println("count search data: " + df_search.count()) // success
    println("count menhu data: " + df_menhu.count()) // success
    println("count total data: " + df.count()) // ERROR ==> success
    */

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

    val df2 = df1.explode("segWords", "tokens") { segWords: String => segWords.split(" ") }
    //    df2.printSchema()

    val df3 = df2.join(posnegDF, df2("tokens") === posnegDF("term"), "left").na.drop()
    val df4 = df3.groupBy("articleId").agg(sum("weight")).withColumnRenamed("sum(weight)", "score")

    val df5 = df4.join(df, Seq("articleId"), "left").drop("contentPre").
      //      withColumn("IS_COMMENT", col("IS_COMMENT").cast("string")).
      withColumn("systime", current_timestamp()).withColumn("systime", date_format($"systime", "yyyy-MM-dd HH:mm:ss"))

    val mainDF = df5.na.drop(Array("title")).drop("content")
    val slaveDF = df.na.drop(Array("content")).select("articleId", "content")

    //    df4.printSchema()
    //    df5.printSchema()
    //    println("全部数据数目为：" + df5.count())
    //    df5.show(5)
    // truncate Mysql Table
    mysqlUtil.truncateMysql(url, user, password, masterTable)
    mysqlUtil.truncateMysql(url, user, password, slaveTable)

    // save Mysql Data
    mysqlUtil.saveMysqlData(mainDF, url, user, password, masterTable, "append")
    mysqlUtil.saveMysqlData(slaveDF, url, user, password, slaveTable, "append")


    sc.stop()
    spark.stop()


  }

}
