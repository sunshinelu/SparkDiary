package com.evayInfo.Inglory.Project.sentiment.dataClean

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup

/**
 * Created by sunlu on 17/7/10.
 *
 *
 * `DA_WEIBO`：
  `ID`：微博ID
  `TEXT`：微博内容
  `REPOSTSCOUNT`：转发数
  `COMMENTSCOUNT`：评论数
  `CREATEDAT`：发表时间
  `UID`：微博作者ID
  `TITLE`：标题
  `WEIBO_KEY`：关键字
 *
 * 修改为：
 *
 * `DA_WEIBO`中获取的数据为：
  `ID`（微博ID）
   对`TEXT`（微博内容）进行正文提取后结果
   `CREATEDAT`（发表时间）
   `WEIBO_KEY`（关键字）
   新增一列`SOURCE`（来源）列：来源为`WEIBO`
 *
 *
 * ＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝
 *
 * `DA_WEIBO_COMMENTS`：
  `ID`：评论ID
  `TEXT`：评论内容
  `WEIBO_ID`：微博ID
  `CREATED_AT`： 发表时间
  `UID`： 评论人ID
  `SCREEN_NAME`：评论人昵称
  `SOURCE`：来源设备
 *
 *
 * 修改为：
 *
 * `DA_WEIBO_COMMENTS`中获取的数据为：
  `ID`（评论ID）
  `WEIBO_ID`：微博ID
  对`TEXT`（评论内容）进行正文提取后结果
  `CREATED_AT`： 发表时间
  `WEIBO_KEY`（关键字）：通过`WEIBO_ID`从`DA_WEIBO`表中`WEIBO_KEY`列获取。
   新增一列`SOURCE`（来源）列：来源为`WEIBO`
 *
 */
object dc_weibo {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"dc_weibo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //    val url = "jdbc:mysql://localhost:3306/bbs"
    val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    // get DA_WEIBO
    val df_w = mysqlUtil.getMysqlData(spark, url, user, password, "DA_WEIBO")
    // get DA_WEIBO
    val df_c = mysqlUtil.getMysqlData(spark, url, user, password, "DA_WEIBO_COMMENTS")
    // select columns
    val df_w_1 = df_w.select("ID", "TEXT", "CREATEDAT", "WEIBO_KEY").withColumn("WEIBO_ID", col("ID"))
    val df_c_1 = df_c.select("ID", "WEIBO_ID", "TEXT", "CREATED_AT")
    // 通过`WEIBO_ID`从`DA_WEIBO`表中`WEIBO_KEY`列获取。
    val keyLib = df_w_1.select("WEIBO_ID", "WEIBO_KEY")
    val df_c_2 = df_c_1.join(keyLib, Seq("WEIBO_ID"), "left").drop("WEIBO_ID")
    val df_w_2 = df_w_1.drop("WEIBO_ID")

    // add IS_COMMENT column
    val addIsComm = udf((arg: Int) => arg)
    val df_w_3 = df_w_2.withColumn("IS_COMMENT", addIsComm(lit(0)))
    val df_c_3 = df_c_2.withColumn("IS_COMMENT", lit(1))


    // change all columns name
    val colRenamed = Seq("ID", "TEXT", "DATE", "TOPIC", "IS_COMMENT")
    val df_w_4 = df_w_3.toDF(colRenamed: _*)
    val df_c_4 = df_c_3.toDF(colRenamed: _*)

    // 合并 df_w_4 和 df_c_4
    val df = df_w_4.union(df_c_4)
    /*
    root
     |-- ID: string (nullable = false)
     |-- TEXT: string (nullable = true)
     |-- DATE: string (nullable = true)
     |-- TOPIC: string (nullable = true)
     */
    // add source column
    val addSource = udf((arg: String) => "WEIBO")
    val df1 = df.withColumn("SOURCE", addSource($"ID"))

    //使用Jsoup进行字符串处理
    val jsoupExtFunc = udf((content: String) => {
      Jsoup.parse(content.toString).body().text()
    })
    val df2 = df1.withColumn("JsoupExt", jsoupExtFunc(col("TEXT"))).drop("TEXT")
    //df2.select("JsoupExt").take(5).foreach(println)

    // 表情符号的替换
    val emoticonPatten = "\\[[0-9a-zA-Z\\u4e00-\\u9fa5]+\\]".r
    val rmEmtionFunc = udf((arg: String) => {
      emoticonPatten.replaceAllIn(arg, "").mkString("")
    })
    val df3 = df2.withColumn("TEXT_pre", rmEmtionFunc(col("JsoupExt"))).drop("JsoupExt")

    // 提取微博中的正文，并添加系统时间列
    val contentPatten = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+[\\u4e00-\\u9fa5a-zA-Z0-9_：【】,.?:;'\"!，。！“”；？]+|@[^,，：:\\s@]+|#[^#]+#".r
    val getContentFunc = udf((arg: String) => {
      contentPatten.replaceAllIn(arg, "").mkString("")
    })
    val df4 = df3.withColumn("CONTENT", getContentFunc(col("TEXT_pre"))).drop("TEXT_pre").
      withColumn("SysTime", current_timestamp()).withColumn("SysTime", date_format($"SysTime", "yyyy-MM-dd HH:mm:ss"))

    df4.printSchema()
    /*
    root
 |-- ID: string (nullable = false)
 |-- DATE: string (nullable = true)
 |-- TOPIC: string (nullable = true)
 |-- SOURCE: string (nullable = true)
 |-- CONTENT: string (nullable = true)
 |-- SysTime: string (nullable = false)
     */

    df4.select("CONTENT", "SysTime").take(5).foreach(println)






    /*
      // 使用Jsoup进行字符串处理，并删除情感字符
      val emoticonPatten = "\\[[0-9a-zA-Z\\u4e00-\\u9fa5]+\\]".r
      val rmEmtionFunc = udf((arg: String) => {
        emoticonPatten.replaceAllIn(Jsoup.parse(arg).body().text(), "").mkString("")
      })

      val df_w_2 = df_w_1.withColumn("TEXT_pre", rmEmtionFunc(col("TEXT")))
      // 提取微博中的正文
      val contentPatten = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+[\\u4e00-\\u9fa5a-zA-Z0-9_：【】,.?:;'\"!，。！“”；？]+|@[^,，：:\\s@]+|#[^#]+#".r
      val getContentFunc = udf((arg: String) => {
        contentPatten.replaceAllIn(arg, "").mkString("")
      })
      val df_w_3 = df_w_2.withColumn("CONTENT",getContentFunc(col("TEXT")))
      // add source column
      val addSource = udf((arg: String) => "WEIBO")
      val df_w_4 = df_w_3.withColumn("Source", addSource($"WEIBO_ID")).drop("TEXT").drop("TEXT_pre")

      // change all columns name
      val colRenamed1 = Seq("ID", "DATE", "TOPIC","CONTENT", "SOURCE")
      val df_w_5 = df_w_4.toDF(colRenamed1: _*)



      // 使用Jsoup进行字符串处理，并删除情感字符
      val df_c_2 = df_c_1.withColumn("TEXT_pre", rmEmtionFunc(col("TEXT")))
      // 提取微博中的正文
      val df_c_3 = df_c_2.withColumn("CONTENT",getContentFunc(col("TEXT")))
      // add source column
      val df_c_4 = df_c_3.withColumn("Source", addSource($"WEIBO_ID")).drop("TEXT").drop("TEXT_pre")
      // 通过`WEIBO_ID`从`DA_WEIBO`表中`WEIBO_KEY`列获取。
      val keyLib = df_w_4.select("WEIBO_ID", "WEIBO_KEY")
      val df_c_5 = df_c_4.join(keyLib, Seq("WEIBO_ID"), "left").drop("WEIBO_ID")
      // change all columns name
      val colRenamed2 = Seq("ID", "DATE","CONTENT", "SOURCE","TOPIC")
      val df_c_6 = df_c_5.toDF(colRenamed2: _*)


      // 合并df_w_5和df_c_6
      val df = df_w_5.union(df_c_6).na.drop
      println(df.count)
      df.printSchema()
      df.take(5).foreach(println)

  */

    /*

        //使用Jsoup进行字符串处理
        val replaceString = udf((content: String) => {
          Jsoup.parse(content).body().text()
        })

        // 使用Jsoup进行字符串处理，并删除情感字符
        val emoticonPatten = "\\[[0-9a-zA-Z\\u4e00-\\u9fa5]+\\]".r
        val rmEmtionFunc = udf((arg: String) => {
          emoticonPatten.replaceAllIn(Jsoup.parse(arg).body().text(), "").mkString("")
        })

        val df1 = df_w.withColumn("content", rmEmtionFunc(col("TEXT")))
        df1.select("content").take(15).foreach(println)

        val df2 = df_c.withColumn("content", replaceString(col("TEXT")))
        df2.select("content").take(15).foreach(println)



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

        // 获取"#话题#"
        val topicPatten = "#[^#]+#".r
        val getTopicsFunc = udf((content: String) => {
          val userNames = topicPatten.findAllMatchIn(content).mkString(";")
          userNames
        })

        val df1_get = df1.withColumn("topics", getTopicsFunc(col("content")))
        df1_get.select("topics").take(15).foreach(println)

    */

    sc.stop()
    spark.stop()
  }

}
