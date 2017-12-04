package com.evayInfo.Inglory.Project.UserProfile

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/12/4.
 * 任务执行代码
 *
spark-submit \
--class com.evayInfo.Inglory.Project.UserProfile.RetentionSave \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 2g \
/root/lulu/Progect/Test/SparkDiary.jar

 */
object RetentionSave {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class RetentionSchema(oneDay:Double,threeDay:Double,sevenDay:Double,fifthDay:Double,oneMonth:Double,
                             twoMonth:Double,threeMonth:Double,fourMonth:Double,fiveMonth:Double,sixMonth:Double)

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"RetentionSave")//.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 表名
    val registTable = "AC_OPERATOR"
    val hbaseTable = "t_hbaseSink"
    val mysqlTable = "YLZX_USERPROFILE_RETENTION"

    // 链接mysql数据库
    val url1 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver")
    prop1.setProperty("user", "ylzx")
    prop1.setProperty("password", "ylzx")

    // 读取注册数据
    val registDF = spark.read.jdbc(url1, registTable, prop1)

    // 读取登陆数据
    val loginRDD  = getLastLogin.getLoginRDD(hbaseTable, sc)
    val loginDF = spark.createDataset(loginRDD).withColumnRenamed("userID", "OPERATOR_ID").na.drop()

    // 获取用户数量
    val userNumber = registDF.select("OPERATOR_ID").distinct().count().toDouble

   /*
   计算留存率
    */
    // 次日留存率
    val df_1d = registDF.select("OPERATOR_ID","START_DATE").withColumn("one",date_add($"START_DATE", 1)).na.drop
    val oneDayRetention = (loginDF.withColumnRenamed("loginTime","one").join(df_1d,Seq("OPERATOR_ID","one"),"inner")
      .na.drop().count() ).toInt / userNumber

    // 3日留存率
    val df_3d = registDF.select("OPERATOR_ID","START_DATE").withColumn("three",date_add($"START_DATE", 3)).na.drop
    val threeDayRetention = (loginDF.withColumnRenamed("loginTime","three").join(df_3d,Seq("OPERATOR_ID","three"),"inner")
      .na.drop().count()).toInt / userNumber

    // 7日留存率
    val df_7d = registDF.select("OPERATOR_ID","START_DATE").withColumn("seven",date_add($"START_DATE", 7)).na.drop
    val sevenDayRetention = (loginDF.withColumnRenamed("loginTime","seven").join(df_7d,Seq("OPERATOR_ID","seven"),"inner")
      .na.drop().count()).toInt / userNumber


    // 15天留存率
    val df3_15d = registDF.select("OPERATOR_ID","START_DATE").withColumn("fifth",date_add($"START_DATE", 15)).na.drop
    val fifthDayRetention = (loginDF.withColumnRenamed("loginTime","fifth").join(df3_15d,Seq("OPERATOR_ID","fifth"),"inner")
      .na.drop().count()).toInt / userNumber


    // 1月留存率
    val df_1m = registDF.select("OPERATOR_ID","START_DATE").withColumn("oneMonth",date_add($"START_DATE", 30)).na.drop
    val oneMonthRetention = (loginDF.withColumnRenamed("loginTime","oneMonth").join(df_1m,Seq("OPERATOR_ID","oneMonth"),"inner")
      .na.drop().count()).toInt / userNumber
    println("oneMonthRetention is: " + oneMonthRetention)

    // 2月留存率
    val df_2m = registDF.select("OPERATOR_ID","START_DATE").withColumn("twoMonth",date_add($"START_DATE", 60)).na.drop
    val twoMonthRetention = (loginDF.withColumnRenamed("loginTime","twoMonth").join(df_2m,Seq("OPERATOR_ID","twoMonth"),"inner")
      .na.drop().count()).toInt / userNumber


    // 3月留存率
    val df_3m = registDF.select("OPERATOR_ID","START_DATE").withColumn("threeMonth",date_add($"START_DATE", 90)).na.drop
    val threeMonthRetention = (loginDF.withColumnRenamed("loginTime","threeMonth").join(df_3m,Seq("OPERATOR_ID","threeMonth"),"inner")
      .na.drop().count()).toInt / userNumber


    // 4月留存率
    val df_4m = registDF.select("OPERATOR_ID","START_DATE").withColumn("fourMonth",date_add($"START_DATE", 120)).na.drop
    val fourMonthRetention = (loginDF.withColumnRenamed("loginTime","fourMonth").join(df_4m,Seq("OPERATOR_ID","fourMonth"),"inner")
      .na.drop().count()).toInt / userNumber

    // 5月留存率
    val df_5m = registDF.select("OPERATOR_ID","START_DATE").withColumn("fiveMonth",date_add($"START_DATE", 150)).na.drop
    val fiveMonthRetention = (loginDF.withColumnRenamed("loginTime","fiveMonth").join(df_5m,Seq("OPERATOR_ID","fiveMonth"),"inner")
      .na.drop().count()).toInt / userNumber

    // 6月留存率
    val df_6m = registDF.select("OPERATOR_ID","START_DATE").withColumn("sixMonth",date_add($"START_DATE", 180)).na.drop
    val sixMonthRetention = (loginDF.withColumnRenamed("loginTime","sixMonth").join(df_6m,Seq("OPERATOR_ID","sixMonth"),"inner")
      .na.drop().count()).toInt / userNumber

    val retentionDF = spark.createDataFrame(Seq(
      RetentionSchema(oneDayRetention, threeDayRetention, sevenDayRetention, fifthDayRetention ,oneMonthRetention,
        twoMonthRetention, threeMonthRetention, fourMonthRetention, fiveMonthRetention, sixMonthRetention)))
      .withColumn("oneDay", bround($"oneDay", 4))
      .withColumn("threeDay", bround($"threeDay", 4))
      .withColumn("sevenDay", bround($"sevenDay", 4))
      .withColumn("fifthDay", bround($"fifthDay", 4))
      .withColumn("oneMonth", bround($"oneMonth", 4))
      .withColumn("twoMonth", bround($"twoMonth", 4))
      .withColumn("threeMonth", bround($"threeMonth", 4))
      .withColumn("fourMonth", bround($"fourMonth", 4))
      .withColumn("fiveMonth", bround($"fiveMonth", 4))
      .withColumn("sixMonth", bround($"sixMonth", 4))
      .withColumn("currTime", current_timestamp()).withColumn("currTime", date_format($"currTime", "yyyy-MM-dd HH:mm:ss"))

    //将retentionDF保存到mysql数据库中
    val url2 = "jdbc:mysql://192.168.37.18:3306/recommender_test?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    retentionDF.write.mode("append").jdbc(url2, mysqlTable, prop2)



    sc.stop()
    spark.stop()
  }

}
