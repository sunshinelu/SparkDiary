package com.evayInfo.Inglory.Project.UserProfile

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/11/28.
 * 读取mysql数据，计算留存率
 *
 * 所需数据
 * 用户信息表：`AC_OPERATOR`
          START_DATE：注册时间
          LAST_LOGIN：最近登陆时间(表中此列无数据)

从t_hbaseSink表中获取用户的登陆时间数据
  读取t_hbaseSink表中的用户ID和登陆时间，并将结果保存到mysql数据库中（YLZX_LAST_LOGIN）
  userID:用户ID
  loginTime:登陆时间（yyyy-MM-dd）

 */
object Retention {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"Retention").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 表名
    val registTable = "AC_OPERATOR"
    val loginTable = "YLZX_LAST_LOGIN"

    // 链接mysql数据库
    val url1 = "jdbc:mysql://localhost:3306/ylzx?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver")
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    // 读取数据
    val registDF = spark.read.jdbc(url1, registTable, prop1)
    val loginDF = spark.read.jdbc(url1, loginTable, prop1).withColumnRenamed("userID", "OPERATOR_ID").na.drop()

    /*
    分析不同时间段用户注册情况(按月统计)
    所需字段：
    OPERATOR_ID：用户ID
    START_DATE：注册时间
     */

    val df1 = registDF.select("OPERATOR_ID","START_DATE").
      withColumn("START_DATE_day", date_format($"START_DATE", "yyyy-MM")).na.drop().
      withColumn("tag",lit(1)).groupBy("START_DATE_day").agg(sum("tag")).withColumnRenamed("sum(tag)","sum").
      sort($"START_DATE_day")
    df1.show(10000,false)
    println("df1的数量为：" + df1.count())
    /*
+--------------+---+
|START_DATE_day|sum|
+--------------+---+
|2017-04       |6  |
|2017-05       |28 |
|2017-06       |22 |
|2017-07       |17 |
|2017-08       |47 |
|2017-09       |237|
|2017-10       |8  |
|2017-11       |7  |
+--------------+---+
     */

    /*
    登陆时间分析（以天为单位）
    注册时间
    最近登陆时间
    现在
    （最近登陆时间－注册时间 ＋ 1）／（现在－最近登陆时间 ＋ 1）
    所需字段：
    OPERATOR_ID：用户ID
    START_DATE：注册时间
    LAST_LOGIN：最近登陆时间
     */

    val lastloginDF = loginDF.sort(col("loginTime"))
    lastloginDF.show(false)
    lastloginDF.printSchema()

    val df2 = registDF.select("OPERATOR_ID","START_DATE").join(loginDF, Seq("OPERATOR_ID"), "left")
      // 获取当前时间
    .withColumn("currTime", current_timestamp()).withColumn("currTime", date_format($"currTime", "yyyy-MM-dd HH:mm:ss")).na.drop()
//      .withColumn("diff", (datediff(col("currTime"), col("START_DATE")) + 1)/(datediff(col("currTime"), col("LAST_LOGIN")) + 1))
    df2.show(false)
    df2.printSchema()


    sc.stop()
    spark.stop()
  }
}
