package com.evayInfo.Inglory.Project.sentiment

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jsoup.Jsoup

/**
 * Created by sunlu on 17/7/14.
 *
DA_WEIBO:
  ID: 微博ID
  TEXT: 微博内容
  REPOSTSCOUNT: 转发数
  COMMENTSCOUNT: 评论数
  CREATEDAT: 发表时间
  UID: 微博作者ID

DA_WEIBO_COMMENTS:
  ID: 评论ID
  TEXT: 评论内容
  WEIBO_ID: 微博ID
  CREATED_AT: 发表时间
  UID: 评论人ID
  SCREEN_NAME: 评论人昵称
  SOURCE: 来源设备

DA_WEIBO_USER
  ID: 微博ID
  SCREENNAME: 微博昵称
  REMARKS: 备注
  CREATE_TIME: 创建时间,
  DEL_FLAG: 0:未删除   1:删除'

 *
 *
 */
object wHotTopic {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def getMysqlData(spark: SparkSession, url: String, user: String, password: String, tableName: String): DataFrame = {
    //connect mysql database
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    //get data
    val df = spark.read.jdbc(url, tableName, prop)
    df
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"wHotTopic").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


    val url = "jdbc:mysql://localhost:3306/bbs"
    val user = "root"
    val password = "root"
    val df_w = getMysqlData(spark, url, user, password, "DA_WEIBO")
    val df_c = getMysqlData(spark, url, user, password, "DA_WEIBO_COMMENTS")
    val df_u = getMysqlData(spark, url, user, password, "DA_WEIBO_USER")
    //    df_w.printSchema()
    //    df_c.printSchema()
    //    df_u.printSchema()

    //使用Jsoup进行字符串处理
    val replaceString = udf((content: String) => {
      Jsoup.parse(content).body().text()
    })

    val df1 = df_w.withColumn("content", replaceString(col("TEXT")))
    df1.select("content").take(15).foreach(println)

    val df2 = df_c.withColumn("content", replaceString(col("TEXT")))
    df2.select("content").take(10).foreach(println)

    // 获取"@用户名"

    val userPatten = "@[\\u4e00-\\u9fa5a-zA-Z0-9_-]{4,30}".r
    //val userPatten = "@[^,，：:\\s@]+".r//这种

    def getUsers(content: String): String = {
      val userNames = userPatten.findAllMatchIn(content).mkString(";")
      userNames
    }
    val getUserFunc2 = udf((arg: String) => getUsers(arg))

    val getUserFunc = udf((content: String) => {
      val userNames = userPatten.findAllMatchIn(content).mkString(";")
      userNames
    })

    val df3 = df1.withColumn("userNames", getUserFunc(col("content")))
    df3.select("userNames").take(15).foreach(println)
    val df3_2 = df1.withColumn("userNames", getUserFunc2(col("content")))
    df3_2.select("userNames").take(15).foreach(println)



    // 获取"#话题#"
    val topicPatten = "#[^#]+#".r
    val getTopicsFunc = udf((content: String) => {
      val userNames = topicPatten.findAllMatchIn(content).mkString(";")
      userNames
    })

    val df4 = df3.withColumn("topics", getTopicsFunc(col("content")))
    df4.select("topics").take(15).foreach(println)


    /*
    val reg = """((http|https|ftp|ftps):\\/\\/)?([a-zA-Z0-9-]+\\.){1,5}(com|cn|net|org|hk|tw)((\\/(\\w|-)+(\\.([a-zA-Z]+))?)+)?(\\/)?(\\??([\\.%:a-zA-Z0-9_-]+=[#\\.%:a-zA-Z0-9_-]+(&amp;)?)+)?""".r

    val s = "今晚派个有点儿帅的你何陪你。<a href='https://m.weibo.cn/n/快乐大本营'>@快乐大本营</a>  <a class='k' href='https://m.weibo.cn/k/快本二十正青春?from=feed'>#快本二十正青春#</a><a class='k' href='https://m.weibo.cn/k/变形金刚5?from=feed'>#变形金刚5#</a> ​​​".toString
    val s1 = reg.replaceAllIn(s,"")
    println(s1)

    println("=======")
    val reg2 = """[\u4e00-\u9fa5]*""".r
    val s2 = reg2.findAllMatchIn(s).mkString("")
    println(s2)

    println("=======")
    val s3 = Jsoup.parse(s).body().text()
    println(s3)

*/


  }

}
