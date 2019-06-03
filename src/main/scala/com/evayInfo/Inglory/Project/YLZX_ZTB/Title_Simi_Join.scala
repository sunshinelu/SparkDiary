package com.evayInfo.Inglory.Project.YLZX_ZTB

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object Title_Simi_Join {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    SetLogger

    //bulid environment
    val spark = SparkSession.builder.appName("DocSimi_Title").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://10.20.5.49:3306/efp5-ztb"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "BigData@2018")
    //get data
    val ds0 = spark.read.jdbc(url1, "collect_ccgp", prop1)
    println(ds0.count()) // 3886   4074


    val ds1 = spark.read.jdbc(url1, "collect_ccgp", prop1).dropDuplicates(Seq("title","website"))
    println(ds1.count()) //3539  3668

    val col_names = Seq("id","title", "data", "website").map(col(_))
    val ds1_0 = ds1.select(col_names: _*).na.drop()

    def time_func(time: String): String = {
      val dateFormat_s1 = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val result = if (time.length() == 19) {
        val data_D_s1 = dateFormat_s1.parse(time)
        val data_S_s1 = dateFormat.format(data_D_s1)
        return data_S_s1
      } else if (time.length() == 10) {
        val data_D_s2 = dateFormat.parse(time)
        val data_S_s2 = dateFormat.format(data_D_s2)
        return data_S_s2
      } else {
        ""
      }
      return result
    }

    val time_udf = udf((time: String) => time_func(time))

    // 使用trim函数去除data列里面的空格
    val ds1_1 = ds1_0.withColumn("data", trim($"data")).
      withColumn("unified_time", time_udf($"data")).
      withColumn("unified_time", $"unified_time".cast(TimestampType)).
      withColumn("unified_time", date_format($"unified_time", "yyyy-MM-dd")).
      orderBy($"unified_time".desc).drop("data")
    val ds1_2 = ds1_1.filter($"unified_time" =!= "2019-05-10")

    // 使用trim函数去除data列里面的空格
    val ds2 = ds1_2.withColumn("title", trim($"title")).withColumn("website", trim($"website"))

    val ds_tag = ds2.select("title","website").
      withColumn("tag", lit("join_tag")).
      withColumnRenamed("title","title_tag").
      withColumnRenamed("website","website_tag")

    val ds3 = ds2.withColumn("tag", lit("join_tag"))

    val ds4 = ds3.join(ds_tag, Seq("tag"),"left")

    val ds5 = ds4.filter($"title" === $"title_tag" && $"website" =!= $"website_tag").na.drop()

    println(ds5.count()) // 4477(X)  2136  2270

    val ds6 = ds5.filter($"website" === "山东政府采购网").select("title")
    val ds6_0 = ds1_2.filter($"website" === "山东政府采购网").select("title")

    println(ds6.count()) //1068  1135

    println(ds6_0.count()) // 1141  1141



    //    val ds7 = ds6_0.join(ds6,Seq("website"),"cross")
    val ds7 = ds6_0.except(ds6)
    println(ds7.count()) // 73 6
    ds7.show(truncate = false)

    val ds8 = ds7.join(ds0, Seq("title"), "left").select(col_names: _*).na.drop().
      dropDuplicates(Seq("title","website"))
    println(ds8.count()) // 77  7
    ds8.write.mode("overwrite").jdbc(url1, "sunlu_diff_title_0603", prop1)

//    val s = "山东省潍坊市寿光市殡仪馆服务大厅建设项目中标公告"  // http://www.ccgp-shandong.gov.cn/sdgp2017/site/read.jsp?colcode=02&id=201072239
    val s = "青岛职业技术学院学前教育专业智慧实训室建设项目更正公告"
    val ds_check = ds1_0.filter($"title" === s)
    ds_check.show(truncate = false)

    /*
    val ds7 = ds6.join(ds6_0,Seq("website"),"except")
java.lang.IllegalArgumentException: Unsupported join type 'except'. Supported join types include: 'inner', 'outer', 'full', 'fullouter', 'leftouter', 'left', 'rightouter', 'right', 'leftsemi', 'leftanti', 'cross'.
  at org.apache.spark.sql.catalyst.plans.JoinType$.apply(joinTypes.scala:42)
  at org.apache.spark.sql.Dataset.join(Dataset.scala:772)
  ... 50 elided

     */


    /*
    //将ds1保存到testTable2表中
    val url2 = "jdbc:mysql://localhost:3306/ylzx_ztb?useUnicode=true&characterEncoding=UTF-8"
    val prop2 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")

    //将结果保存到数据框中
    ds1.write.mode("overwrite").jdbc(url2, "collect_ccgp", prop2) //overwrite  append
    */
    sc.stop()
    spark.stop()
  }

}
