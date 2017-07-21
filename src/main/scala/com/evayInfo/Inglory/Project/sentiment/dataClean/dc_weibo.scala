package com.evayInfo.Inglory.Project.sentiment.dataClean

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup

/**
 * Created by sunlu on 17/7/21.
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
1) `DA_WEIBO`中获取的数据为：
  `ID`（微博ID）
   `TITLE`（标题）
   `TEXT`（微博内容）
   `CREATEDAT`（发表时间）
   `WEIBO_KEY`（关键字）
   新增一列`SOURCE`（来源）列：来源为`WEIBO`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
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
2) `DA_WEIBO_COMMENTS`中获取的数据为：
  `ID`（评论ID）
  `WEIBO_ID`：微博ID
  `TITLE`（标题）：通过`WEIBO_ID`从`DA_WEIBO`表中`TITLE`列获取。
  `TEXT`（评论内容）
  `CREATED_AT`： 发表时间
  `WEIBO_KEY`（关键字）：通过`WEIBO_ID`从`DA_WEIBO`表中`WEIBO_KEY`列获取。
   新增一列`SOURCE`（来源）列：来源为`WEIBO`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
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
    // get DA_WEIBO_COMMENTS
    val df_c = mysqlUtil.getMysqlData(spark, url, user, password, "DA_WEIBO_COMMENTS")
    // select columns
    val df_w_1 = df_w.select("ID", "TITLE", "TEXT", "CREATEDAT", "WEIBO_KEY").withColumn("WEIBO_ID", col("ID"))
    val df_c_1 = df_c.select("ID", "WEIBO_ID", "TEXT", "CREATED_AT")
    // 通过`WEIBO_ID`从`DA_WEIBO`表中`WEIBO_KEY`列获取。
    val keyLib = df_w_1.select("WEIBO_ID", "TITLE", "WEIBO_KEY")
    val df_c_2 = df_c_1.join(keyLib, Seq("WEIBO_ID"), "left").select("ID", "TITLE", "TEXT", "CREATED_AT", "WEIBO_KEY")
    val df_w_2 = df_w_1.drop("WEIBO_ID")

    // add IS_COMMENT column
    val addIsComm = udf((arg: Int) => arg)
    val df_w_3 = df_w_2.withColumn("IS_COMMENT", addIsComm(lit(0)))
    val df_c_3 = df_c_2.withColumn("IS_COMMENT", lit(1))

    // change all columns name
    val colRenamed = Seq("ARTICLEID", "TITLE", "TEXT", "TIME", "KEYWORD", "IS_COMMENT")
    val df_w_4 = df_w_3.toDF(colRenamed: _*)
    val df_c_4 = df_c_3.toDF(colRenamed: _*)

    // 合并 df_w_4 和 df_c_4
    val df = df_w_4.union(df_c_4)

    // add source column
    val addSource = udf((arg: String) => "WEIBO")
    val df1 = df.withColumn("SOURCE", addSource($"ARTICLEID"))

    //使用Jsoup进行字符串处理
    val jsoupExtFunc = udf((content: String) => {
      Jsoup.parse(content.toString).body().text()
    })
    val df2 = df1.withColumn("JsoupExt", jsoupExtFunc(col("TEXT")))
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
    val df4 = df3.withColumn("CONTENT", getContentFunc(col("TEXT_pre"))).drop("TEXT_pre").na.drop(Array("CONTENT")) //.
    //      withColumn("SYSTIME", current_timestamp()).withColumn("SYSTIME", date_format($"SYSTIME", "yyyy-MM-dd HH:mm:ss"))

    df4.printSchema()
    /*
 |-- ARTICLEID: string (nullable = false)
 |-- TITLE: string (nullable = true)
 |-- TEXT: string (nullable = true)
 |-- TIME: string (nullable = true)
 |-- KEYWORD: string (nullable = true)
 |-- IS_COMMENT: integer (nullable = true)
 |-- SOURCE: string (nullable = true)
 |-- CONTENT: string (nullable = true)
     */

    //    df4.select("CONTENT", "SysTime").take(5).foreach(println)


    sc.stop()
    spark.stop()
  }

  /*
getWeiboData：获取清洗后的微博数据全部数据
*/
  def getWeiboData(spark: SparkSession, url: String, user: String, password: String,
                   wTable: String, wCommentTable: String): DataFrame = {

    // get DA_WEIBO
    val df_w = mysqlUtil.getMysqlData(spark, url, user, password, wTable)
    // get DA_WEIBO_COMMENTS
    val df_c = mysqlUtil.getMysqlData(spark, url, user, password, wCommentTable)
    // select columns
    val df_w_1 = df_w.select("ID", "TITLE", "TEXT", "CREATEDAT", "WEIBO_KEY").withColumn("WEIBO_ID", col("ID"))
    val df_c_1 = df_c.select("ID", "WEIBO_ID", "TEXT", "CREATED_AT")
    // 通过`WEIBO_ID`从`DA_WEIBO`表中`WEIBO_KEY`列获取。
    val keyLib = df_w_1.select("WEIBO_ID", "TITLE", "WEIBO_KEY")
    val df_c_2 = df_c_1.join(keyLib, Seq("WEIBO_ID"), "left").select("ID", "TITLE", "TEXT", "CREATED_AT", "WEIBO_KEY")
    val df_w_2 = df_w_1.drop("WEIBO_ID")

    // add IS_COMMENT column
    val addIsComm = udf((arg: Int) => arg)
    val df_w_3 = df_w_2.withColumn("IS_COMMENT", addIsComm(lit(0)))
    val df_c_3 = df_c_2.withColumn("IS_COMMENT", lit(1))

    // change all columns name
    val colRenamed = Seq("ARTICLEID", "TITLE", "TEXT", "TIME", "KEYWORD", "IS_COMMENT")
    val df_w_4 = df_w_3.toDF(colRenamed: _*)
    val df_c_4 = df_c_3.toDF(colRenamed: _*)

    // 合并 df_w_4 和 df_c_4
    val df = df_w_4.union(df_c_4)

    // add source column
    val df1 = df.withColumn("SOURCE", lit("WEIBO")).na.drop(Array("TEXT")).filter(length(col("TEXT")) >= 5)

    //使用Jsoup进行字符串处理
    val jsoupExtFunc = udf((content: String) => {
      Jsoup.parse(content).body().text()
    })

    def jsoupExtFunc2(content: String): String = {
      val jsoupExt = Jsoup.parse(content).body().text()
      jsoupExt
    }
    val jsoupExtUdf = udf((arg: String) => jsoupExtFunc2(arg))

    val df2 = df1.withColumn("JsoupExt", jsoupExtFunc(col("TEXT")))
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
    val df4 = df3.withColumn("CONTENT", getContentFunc(col("TEXT_pre"))).drop("TEXT_pre").na.drop(Array("CONTENT"))
    df4
  }


}
