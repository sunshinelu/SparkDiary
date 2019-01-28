package com.evayInfo.Inglory.Project.RenCai.PerformanceTest

import java.text.SimpleDateFormat
import java.util.Properties

import breeze.linalg.{max, min}
import com.evayInfo.Inglory.Project.RenCai.XiaoYouRelation.{SetLogger, degree}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object XiaoYouRelationTest {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def degree(s1:String, e1:String, s2:String, e2:String):Double={
    //定义时间格式
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    val latest_start = max(dateFormat.parse(s1).getTime,dateFormat.parse(s2).getTime)
    val earliest_end = min(dateFormat.parse(e1).getTime, dateFormat.parse(e2).getTime)
    val overlap = (earliest_end - latest_start)/(1000*3600*24) +1
    val result = if (overlap < 0){
      0.0
    }else{
      overlap / (3 * 365.0)
    }

    val degree_result = if (result > 1){
      100.0
    }else{
      100.0 * result
    }
    return degree_result
  }

  def main(args: Array[String]): Unit = {

    // 获取当前时间
    val d0 = new java.util.Date()
    println("任务启动时间 " + d0.getTime)

    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"TongXueRelation").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/talent"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    val to_lower_udf = udf((x: String) => x.toLowerCase())

    //get data
    val ds1 = spark.read.jdbc(url1, "education_info", prop1)
    val ds2 = ds1.select("talent_id","school_name","start_date","end_date").na.drop().
      withColumn("talent_id", to_lower_udf($"talent_id")).dropDuplicates()

    val id = "502D58D0-EA9E-4521-8A31-06E5E7AA13AA".toLowerCase()
    val col_temp_1 = Seq("id_1","school_name","s1","e1")
    val temp_1 = ds2.toDF(col_temp_1: _*).filter($"id_1" === id)

    val col_temp_2 = Seq("id_2","school_name","s2","e2")
    val temp_2 = ds2.toDF(col_temp_2: _*)

    val temp_3 = temp_1.join(temp_2,Seq("school_name"),"outer").filter($"id_1" =!= $"id_2").na.drop()

    val degree_udf = udf((s1:String, e1:String, s2:String, e2:String) => degree(s1,e1,s2,e2))

    val temp_4 = temp_3.withColumn("degree",degree_udf($"s1",$"e1",$"s2",$"e2"))

    val temp_5 = temp_4.na.drop().select("school_name", "id_1", "id_2", "degree").
      groupBy("id_1", "id_2", "school_name").agg(sum("degree"))

    def maxWeight(x: Double): Double = {
      val result = if (x > 100) {
        100.0
      } else
        x
      return result
    }

    val maxWeight_udf = udf((x: Double) => maxWeight(x))

    val temp_6 = temp_5.withColumn("degree", bround(maxWeight_udf($"sum(degree)"),3))

    //get data
    val info_ds = spark.read.jdbc(url1, "talent_info_new", prop1).
      select("talent_id","name").
      withColumn("talent_id", to_lower_udf($"talent_id")).
      dropDuplicates()

    val info_id1 = info_ds.toDF("id_1","name_1")
    val info_id2 = info_ds.toDF("id_2","name_2")

    val join_df = temp_6.join(info_id1,Seq("id_1"),"left").join(info_id2,Seq("id_2"),"left")

    val ds3 = join_df.withColumn("relation",lit("校友")).
      withColumn("create_time", current_timestamp()).
      withColumn("create_time", date_format($"create_time", "yyyy-MM-dd HH:mm:ss")).
      withColumn("update_time",$"create_time")

    val result_col = Seq("source_id","source_name","target_id","target_name","relation","relation_object","weight","create_time","update_time")
    val ds4 = ds3.select("id_1","name_1","id_2","name_2","relation","school_name","degree","create_time","update_time").
      toDF(result_col:_*).dropDuplicates().na.drop()
    val result_df = ds4.select("target_id","target_name","source_id","source_name","relation","relation_object","weight","create_time","update_time").
      toDF(result_col:_*).union(ds4)

//    result_df.filter($"target_id" === "502D58D0-EA9E-4521-8A31-06E5E7AA13AA".toLowerCase()).show(truncate = false)

    result_df.show(truncate = false)


    // 获取时间差
    val d3 = new java.util.Date()
    println("计算相似性时间： " + d3.getTime)
    val diff = d3.getTime - d0.getTime // 返回自此Date对象表示的1970年1月1日，00:00:00 GMT以来的毫秒数。
    val diffMinutes = diff / (1000.0) // 时间间隔，单位：秒
    println("时间间隔(单位：秒): " + diffMinutes)
    println("时间间隔(单位：分钟): " + diffMinutes / 60.0)

    sc.stop()
    spark.stop()
  }
}
