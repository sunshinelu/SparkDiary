package com.evayInfo.Inglory.Project.sentiment.sentimentAnalysis

import java.util.UUID

import com.evayInfo.Inglory.Project.sentiment.dataClean._
import com.evayInfo.Inglory.util.mysqlUtil
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Created by sunlu on 18/1/31.
 * 根据李总的要求重新对舆情数据进行分析
 *
spark-submit \
--class com.evayInfo.Inglory.Project.sentiment.sentimentAnalysis.sentimentTrendV3 \
--master yarn \
--num-executors 8 \
--executor-cores 6 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar,/root/software/extraClass/mysql-connector-java-5.1.17.jar \
/root/lulu/Progect/sentiment/SparkDiary.jar
 */
object sentimentTrendV3 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"sentimentTrendV3") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val url1 = "jdbc:mysql://192.168.37.104:33333/IngloryBDP?useUnicode=true&characterEncoding=UTF-8"// +
//      "?allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
    val user1 = "root"
    val password1 = "root"

    val weiboAtable = "DA_WEIBO"
    val weiboCtable = "DA_WEIBO_COMMENTS"
    val weixinTable = "DA_WEIXIN"
    val luntanAtable = "DA_BBSARTICLE"
    val luntanCtable = "DA_BBSCOMMENT"
    val searchTable = "DA_BAIDUARTICLE"
    val menhuTable = "DA_SEED_201802"
    val blogTable = "DA_BLOG"

    val url2 = "jdbc:mysql://192.168.37.18:3306/bbs?useUnicode=true&characterEncoding=UTF-8"
    val user2 = "root"
    val password2 = "root"
    val articleTable = "yq_article"
    val contentTable = "yq_content"
    val allTable = "yq_all_new"

    val df_weibo = dc_weibo.getWeiboData2(spark, url1, user1, password1, weiboAtable, weiboCtable)
    val df_weixin = dc_weixin.getWeixinData(spark, url1, user1, password1, weixinTable)
    val df_luntan = dc_luntan.getLuntanData(spark, url1, user1, password1, luntanAtable, luntanCtable)
    val df_search = dc_search.getSearchData(spark, url1, user1, password1, searchTable)
    val df_menhu = dc_menhu.getMenhuData(spark, url1, user1, password1, menhuTable)
    val df_bolg = dc_blog.getBlogData(spark, url1, user1, password1, blogTable)
    val id_df = mysqlUtil.getMysqlData(spark, url2, user2, password2, articleTable).select("ARTICLEID")

//    println("weixin is: " + df_weixin.count())

    val df = df_weibo.union(df_weixin).union(df_luntan).union(df_search).union(df_menhu).union(df_bolg).
      filter(length(col("time")) === 19).join(id_df, Seq("ARTICLEID"), "leftanti").filter(length(col("content")) >= 1)

//    println("df weixin is: " +  df.filter($"source".contains("WEIXIN")).count())

    //获取正类、负类词典。posnegDF在join时使用；posnegList在词过滤时使用。
    val posnegDF = spark.read.format("CSV").option("header", "true").load("/personal/sunlu/Project/sentiment/posneg.csv")
    val posnegList = posnegDF.select("term").dropDuplicates().rdd.map { case Row(term: String) => term }.collect().toList

    // 将posnegList添加到词典中
    posnegList.foreach(x => {
      UserDefineLibrary.insertWord(x, "userDefine", 1000)
    })

    //load stopwords file
    val stopwordsFile = "/personal/sunlu/Project/sentiment/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //定义UDF
    //分词、停用词过滤、正类、负类词过滤
    val segWorsd = udf((content: String) => {
      ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word))//.filter(word => posnegList.contains(word))
        .toSeq.mkString(" ")
    })

    def segWords(content:String):String ={
      val words = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 1).map(_ (0)).toList.
//        filter(word => word.length >= 1 & !stopwords.contains(word)).filter(word => posnegList.contains(word))
        filter(word => word.length >= 1 & posnegList.contains(word))
        .toSeq
      val result = words match {
        case r if (r.length >= 1) => r.mkString(" ")
        case _ => "NULL"
      }
      result
    }
    val segWordsUdf = udf((content:String) => segWords(content))


    val df1 = df.select("articleId", "contentPre").na.drop().
//      withColumn("segWords", segWorsd(column("contentPre")))
      withColumn("segWords", segWordsUdf(column("contentPre")))

    val df2 = df1.explode("segWords", "tokens") { segWords: String => segWords.split(" ") }
//        df2.printSchema()


    //    val df3 = df2.join(posnegDF, df2("tokens") === posnegDF("term"), "left").na.drop()
    val df3 = df2.join(posnegDF, df2("tokens") === posnegDF("term"), "left").na.fill(Map("weight" -> 0))

    val df4 = df3.groupBy("articleId").agg(sum("weight")).withColumnRenamed("sum(weight)", "score")



    val df5 = df4.join(df, Seq("articleId"), "left").drop("contentPre").
      //      withColumn("IS_COMMENT", col("IS_COMMENT").cast("string")).
      withColumn("systime", current_timestamp()).withColumn("systime", date_format($"systime", "yyyy-MM-dd HH:mm:ss"))

//    println("df5 weixin is: " +  df5.filter($"source".contains("WEIXIN")).count())


    //      val mainDF = df5.na.drop(Array("title", "content")).drop("content")
    //      val slaveDF = df5.na.drop(Array("title", "content")).select("articleId", "content")

    val df6 = df5.filter(length(col("title")) >= 1)
    // .filter(length(col("content")) >= 1)
    .dropDuplicates(Array("articleId"))
//    df6.persist()

//    println("df6 weixin is: " +  df6.filter($"source".contains("WEIXIN")).count())


    def uuidFunc():String={
      val uuid = UUID.randomUUID().toString().toLowerCase()
      uuid
    }
    val uuidUDF = udf(() => uuidFunc())

    mysqlUtil.truncateMysql(url2, user2, password2, allTable)
    
    df6.withColumn("id",uuidUDF())
//    .coalesce(1)
    .write.
    format("jdbc")
    .mode(SaveMode.Append)
    .option("dbtable", allTable)
    .option("url", url2)
    .option("user", user2)
    .option("password", password2)
    .option("numPartitions", "10")
    .save()

    val all_df = mysqlUtil.getMysqlData(spark, url2, user2, password2, allTable)
    all_df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val masterDF = all_df.drop("content")//.dropDuplicates(Array("articleId"))
    val slaveDF = all_df.select("articleId", "content")//.dropDuplicates(Array("articleId"))

    println("start save table " + articleTable)
    masterDF.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("dbtable", articleTable)
      .option("url", url2)
      .option("user", user2)
      .option("password", password2)
      .option("numPartitions", "5")
      .save()
    println("succed save table " + articleTable)

    println("start save table " + contentTable)
    slaveDF.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("dbtable", contentTable)
      .option("url", url2)
      .option("user", user2)
      .option("password", password2)
      .option("numPartitions", "5")
      .save()
    println("succed save table " + contentTable)

    sc.stop()
    spark.stop()

  }

}
