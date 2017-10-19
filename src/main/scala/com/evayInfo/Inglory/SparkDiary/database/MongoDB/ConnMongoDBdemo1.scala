package com.evayInfo.Inglory.SparkDiary.database.MongoDB

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/19.
 * 链接MongoDB参考链接：
 *
 * http://www.cnblogs.com/wwxbi/p/7170679.html
 * http://m.blog.csdn.net/github_36869152/article/details/71762699
 *
 */
object ConnMongoDBdemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"ConnMongoDBdemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext


    val uri = "mongodb://172.1.1.1:27017"

    val userDF = spark.sql(
      """
select
       uid,
       name,
       current_date() version
  from test_table
  limit 100
      """).repartition(8)

    // Write to MongoDB
    userDF.write.mode("overwrite").format("com.mongodb.spark.sql").options(
      Map(
        "uri" -> uri,
        "database" -> "test",
        "collection" -> "test_table")).save()

    // Read From MongoDB
    val df = spark.read.format("com.mongodb.spark.sql").options(
      Map(
        "uri" -> uri,
        "database" -> "test",
        "collection" -> "test_table")).load()

    // 其他方式
    userDF.write.mode("overwrite").format("com.mongodb.spark.sql").options(
      Map(
        "spark.mongodb.input.uri" -> uri,
        "spark.mongodb.output.uri" -> uri,
        "spark.mongodb.output.database" -> "test",
        "spark.mongodb.output.collection" -> "test_table")).save()

    MongoSpark.save(
      userDF.write.mode("overwrite").options(
        Map(
          "spark.mongodb.input.uri" -> uri,
          "spark.mongodb.output.uri" -> uri,
          "spark.mongodb.output.database" -> "test",
          "spark.mongodb.output.collection" -> "test_table")))

    MongoSpark.save(
      userDF.write.mode("overwrite").options(
        Map(
          "uri" -> uri,
          "database" -> "test",
          "collection" -> "test_table")))


    sc.stop()
    spark.stop()
  }

}
