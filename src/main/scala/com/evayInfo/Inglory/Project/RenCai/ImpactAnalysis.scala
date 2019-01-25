package com.evayInfo.Inglory.Project.RenCai

import java.util.Properties


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/*

在bigdata7运行

spark-shell --master yarn --num-executors 4 --executor-cores  4 --executor-memory 4g

查看 effect 表中数据量

select count(*) from effect
 */
object ImpactAnalysis {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"ImpactAnalysis").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/talent"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    /*
val url1 = "jdbc:mysql://10.20.7.156:3306/talent?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
val prop1 = new Properties()
prop1.setProperty("user", "root")
prop1.setProperty("password", "rcDsj_56")
 */

    val to_lower_udf = udf((x:String) => x.toLowerCase())

    //get data
    val ds1 = spark.read.jdbc(url1, "pubication_info", prop1)

    val ds2 = ds1.select("id","talent_id", "name", "impact_factor", "reference_number","situation","is_contact").
      withColumn("talent_id", to_lower_udf($"talent_id"))

    val ds3 = ds2.withColumn("is_contact",$"is_contact".cast("int")).
      na.fill(value = 0, cols = Array("reference_number","is_contact")).
      na.fill(value = 0.0, cols = Array("impact_factor")).
      na.fill(value = "null",cols = Array("situation"))

//    println(ds2.count())
//    println(ds2.select("id").distinct().count())
//    println(ds2.select("name").distinct().count())

//    ds3.printSchema()

    val impact_factor_max = 50.00
    val reference_number_max = ds3.agg(max($"reference_number")).first match {
      case Row(y: Int) => (y.toDouble)
    }
//    println(s"impact_factor_max 为$impact_factor_max, reference_number_max为 $reference_number_max")

//    ds3.select("situation").distinct().show(truncate = false)

//    ds3.select($"is_contact").distinct().show(truncate = false)

    /*
    收录情况（不为null即已收录，为1）
     */
    def situation_toValue(x:String):Int={
      val result = if (x == "null"){
        0
      } else 1
      return  result * 30
    }
    val situation_toValue_udf = udf((x:String) => situation_toValue(x))

    val ds4 = ds3.withColumn("situation_value",situation_toValue_udf($"situation")).drop("situation")
//    ds4.show(truncate = false)

    /*
    影响因子/最大的影响因子
     */
    def impact_factor_func(x:Double):Int={
      val v1 = x / impact_factor_max
      val v2 = if(v1 >= 1){
        1
      }else{
        v1
      }
      val result = (v2 * 30).toInt
      return result
    }
    val impact_factor_udf = udf((x:Double)=>impact_factor_func(x))

    /*
他引次数/最大他引次数
     */
    def reference_number_func(x:Int):Int = {
      val v1 = x / reference_number_max
      val result = (v1 * 30).toInt
      return result
    }
    val reference_number_udf = udf((x:Int)=> reference_number_func(x))



    val ds5 = ds4.withColumn("impact_factor_value",impact_factor_udf($"impact_factor")).
      withColumn("reference_number_value",reference_number_udf($"reference_number")).
      drop("impact_factor").drop("reference_number")

    val ds6 = ds5.withColumn("degree", $"impact_factor_value" + $"reference_number_value" + $"situation_value" + $"is_contact" * 10)

    val ds7 = ds6.groupBy("talent_id").agg(max($"degree"))

//    println(s"ds7 的数量为：" + ds7.count()) // ds7 的数量为：9515
//    println(s"talent_id 原始数量为：" + ds1.select("talent_id").distinct().count()) // talent_id 原始数量为：9515

    //get data
    val info_ds = spark.read.jdbc(url1, "talent_info_new", prop1).select("talent_id", "name").
      withColumn("talent_id", to_lower_udf($"talent_id")).
      dropDuplicates()
//    println("==============")
//    info_ds.filter($"talent_id".contains("aabeadce-259e-4792-8b0d-2803f987b556")).show(truncate = false)
//    println("==============")
//    println("----------------")
//    ds7.filter($"talent_id".contains("aabeadce-259e-4792-8b0d-2803f987b556")).show(truncate = false)
//    println("----------------")

    val ds8 = ds7.join(info_ds, Seq("talent_id"), "left").
      withColumn("create_time", current_timestamp()).
      withColumn("create_time", date_format($"create_time", "yyyy-MM-dd HH:mm:ss"))

    val col_names = Seq("talent_id","talent_name","effect","create_time")

    val result_ds = ds8.select("talent_id","name","max(degree)","create_time").toDF(col_names:_*).na.drop()

    val url2 = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    //    val url = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8&" +
    //      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    // 使用"useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"防止出现时间上的错误
    val prop2 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")

    /*
 val url2 = "jdbc:mysql://10.20.7.156:3306/rck?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
val prop2 = new Properties()
prop2.setProperty("user", "root")
prop2.setProperty("password", "rcDsj_56")
 */

    //将结果保存到数据框中
    result_ds.coalesce(10).write.mode("append").jdbc(url2, "effect", prop2) //overwrite




    sc.stop()
    spark.stop()
  }
}
