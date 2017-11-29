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

  case class RetentionSchema(time: String, value: Double)

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
//    df1.show(10000,false)
//    println("df1的数量为：" + df1.count())
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

    val lastloginDF = loginDF.orderBy(col("loginTime").desc).dropDuplicates("OPERATOR_ID")
//    lastloginDF.show(false)
//    lastloginDF.printSchema()

    val df2 = registDF.select("OPERATOR_ID","START_DATE").join(lastloginDF, Seq("OPERATOR_ID"), "left")
      // 获取当前时间
    .withColumn("currTime", current_timestamp()).withColumn("currTime", date_format($"currTime", "yyyy-MM-dd HH:mm:ss")).na.drop()
      .withColumn("diff", bround((datediff(col("currTime"), col("START_DATE")) + 1)/(datediff(col("currTime"), col("loginTime")) + 1), 3)).na.drop
//    df2.show(false)
//    df2.printSchema()


    /*
    留存率分析
     */

    val userNumber = registDF.select("OPERATOR_ID").distinct().count().toDouble
    println("userNumber is: " + userNumber)

    // 次日留存率
    val df3_1 = registDF.select("OPERATOR_ID","START_DATE").withColumn("one",date_add($"START_DATE", 1)).na.drop
    val oneDayRetention = (loginDF.withColumnRenamed("loginTime","one").join(df3_1,Seq("OPERATOR_ID","one"),"inner")
      .na.drop().count() ).toInt / userNumber
    println("oneDayRetention is: " + oneDayRetention)
//    df3_1.show(false)

    // 3日留存率
    val df3_3d = registDF.select("OPERATOR_ID","START_DATE").withColumn("three",date_add($"START_DATE", 3)).na.drop
    val threeDayRetention = (loginDF.withColumnRenamed("loginTime","three").join(df3_3d,Seq("OPERATOR_ID","three"),"inner")
      .na.drop().count()).toInt / userNumber
    println("threeDayRetention is: " + threeDayRetention)

    // 7日留存率
    val df3_7d = registDF.select("OPERATOR_ID","START_DATE").withColumn("seven",date_add($"START_DATE", 7)).na.drop
    val sevenDayRetention = (loginDF.withColumnRenamed("loginTime","seven").join(df3_7d,Seq("OPERATOR_ID","seven"),"inner")
      .na.drop().count()).toInt / userNumber
    println("sevenDayRetention is: " + sevenDayRetention)

    // 15天留存率
    val df3_15d = registDF.select("OPERATOR_ID","START_DATE").withColumn("fifth",date_add($"START_DATE", 15)).na.drop
    val fifthDayRetention = (loginDF.withColumnRenamed("loginTime","fifth").join(df3_15d,Seq("OPERATOR_ID","fifth"),"inner")
      .na.drop().count()).toInt / userNumber
    println("fifthDayRetention is: " + fifthDayRetention)

    // 1月留存率
    val df3_1m = registDF.select("OPERATOR_ID","START_DATE").withColumn("oneMonth",date_add($"START_DATE", 30)).na.drop
    val oneMonthRetention = (loginDF.withColumnRenamed("loginTime","oneMonth").join(df3_1m,Seq("OPERATOR_ID","oneMonth"),"inner")
      .na.drop().count()).toInt / userNumber
    println("oneMonthRetention is: " + oneMonthRetention)

    // 2月留存率
    val df3_2m = registDF.select("OPERATOR_ID","START_DATE").withColumn("twoMonth",date_add($"START_DATE", 60)).na.drop
    val twoMonthRetention = (loginDF.withColumnRenamed("loginTime","twoMonth").join(df3_2m,Seq("OPERATOR_ID","twoMonth"),"inner")
      .na.drop().count()).toInt / userNumber
    println("twoMonthRetention is: " + twoMonthRetention)

    // 3月留存率
    val df3_3m = registDF.select("OPERATOR_ID","START_DATE").withColumn("threeMonth",date_add($"START_DATE", 90)).na.drop
    val threeMonthRetention = (loginDF.withColumnRenamed("loginTime","threeMonth").join(df3_3m,Seq("OPERATOR_ID","threeMonth"),"inner")
      .na.drop().count()).toInt / userNumber
    println("threeMonthRetention is: " + threeMonthRetention)

    // 4月留存率
    val df3_4m = registDF.select("OPERATOR_ID","START_DATE").withColumn("fourMonth",date_add($"START_DATE", 120)).na.drop
    val fourMonthRetention = (loginDF.withColumnRenamed("loginTime","fourMonth").join(df3_4m,Seq("OPERATOR_ID","fourMonth"),"inner")
      .na.drop().count()).toInt / userNumber
    println("fourMonthRetention is: " + fourMonthRetention)

    // 5月留存率
    val df3_5m = registDF.select("OPERATOR_ID","START_DATE").withColumn("fiveMonth",date_add($"START_DATE", 150)).na.drop
    val fiveMonthRetention = (loginDF.withColumnRenamed("loginTime","fiveMonth").join(df3_5m,Seq("OPERATOR_ID","fiveMonth"),"inner")
      .na.drop().count()).toInt / userNumber
    println("fiveMonthRetention is: " + fiveMonthRetention)


    // 6月留存率
    val df3_6m = registDF.select("OPERATOR_ID","START_DATE").withColumn("sixMonth",date_add($"START_DATE", 180)).na.drop
    val sixMonthRetention = (loginDF.withColumnRenamed("loginTime","sixMonth").join(df3_6m,Seq("OPERATOR_ID","sixMonth"),"inner")
      .na.drop().count()).toInt / userNumber
    println("sixMonthRetention is: " + sixMonthRetention)


    // 7月留存率
    val df3_7m = registDF.select("OPERATOR_ID","START_DATE").withColumn("sevenMonth",date_add($"START_DATE", 210)).na.drop
    val sevenMonthRetention = (loginDF.withColumnRenamed("loginTime","sevenMonth").join(df3_7m,Seq("OPERATOR_ID","sevenMonth"),"inner")
      .na.drop().count()).toInt / userNumber
    println("sevenMonthRetention is: " + sevenMonthRetention)

    val retentionDF = spark.createDataFrame(Seq(
      RetentionSchema("oneDayRetention", oneDayRetention), RetentionSchema("threeDayRetention", threeDayRetention),
      RetentionSchema("sevenDayRetention", sevenDayRetention), RetentionSchema("fifthDayRetention", fifthDayRetention),
      RetentionSchema("oneMonthRetention", oneMonthRetention), RetentionSchema("twoMonthRetention", twoMonthRetention),
      RetentionSchema("threeMonthRetention", threeMonthRetention), RetentionSchema("fourMonthRetention", fourMonthRetention),
      RetentionSchema("fiveMonthRetention", fiveMonthRetention), RetentionSchema("sixMonthRetention", sixMonthRetention)))
      .withColumn("value", bround($"value", 4))
    retentionDF.show(false)
    /*
+-------------------+------+
|time               |value |
+-------------------+------+
|oneDayRetention    |0.0021|
|threeDayRetention  |0.0043|
|sevenDayRetention  |0.0043|
|fifthDayRetention  |0.0043|
|oneMonthRetention  |0.0086|
|twoMonthRetention  |0.0107|
|threeMonthRetention|0.0107|
|fourMonthRetention |0.0043|
|fiveMonthRetention |0.0   |
|sixMonthRetention  |0.0   |
+-------------------+------+
     */

    retentionDF.toJSON.show(false)

    sc.stop()
    spark.stop()
  }
}
