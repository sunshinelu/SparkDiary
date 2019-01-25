package com.evayInfo.Inglory.Project.RenCai

import java.text.SimpleDateFormat
import java.util.Properties

import breeze.linalg.{max, min}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/*
在bigdata7运行
spark-shell --master yarn --num-executors 4 --executor-cores  4 --executor-memory 4g

select count(*) from relation

63387072

spark-shell --master yarn --num-executors 8 --executor-cores  4 --executor-memory 8g

select count(*) from relation_new
1147260(未全部转为小写)
67471844（全部转为小写，但是bigdata7忽然断掉链接，任务可能尚未执行完毕！！！）

 */
object XiaoYouRelation {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def calc_overlap_days_2(s1:String, e1:String, s2:String, e2:String):Int={
    //定义时间格式
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    val latest_start = max(dateFormat.parse(s1).getTime,dateFormat.parse(s2).getTime)
    val earliest_end = min(dateFormat.parse(e1).getTime, dateFormat.parse(e2).getTime)
    var overlap = (earliest_end - latest_start)/(1000*3600*24) +1

    if (overlap < 0){
      overlap = 0
    }
    return overlap.toInt
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

    /*
val url1 = "jdbc:mysql://10.20.7.156:3306/talent?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
val prop1 = new Properties()
prop1.setProperty("user", "root")
prop1.setProperty("password", "rcDsj_56")
 */

    val to_lower_udf = udf((x: String) => x.toLowerCase())

    //get data
    val ds1 = spark.read.jdbc(url1, "education_info", prop1)
    val ds2 = ds1.select("talent_id","school_name","start_date","end_date").na.drop().
      withColumn("talent_id", to_lower_udf($"talent_id")).dropDuplicates()

    val col_temp_1 = Seq("id_1","school_name","s1","e1")
    val temp_1 = ds2.toDF(col_temp_1: _*)

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


    //将ds1保存到testTable2表中
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

    result_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //将结果保存到数据框中
    result_df.coalesce(10).write.mode("append").jdbc(url2, "relation_new", prop2) //overwrite








    sc.stop()
    spark.stop()
  }
}
