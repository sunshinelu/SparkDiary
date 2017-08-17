package com.evayInfo.Inglory.Project.DocsSimilarity

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by sunlu on 17/8/17.
 * 获取关键词词典
 *
 */
object getKeyWords {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {


    SetLogger

    val conf = new SparkConf().setAppName(s"sentimentTrendV1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8"
    //    val url = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8"
    val user = "root"
    val password = "root"

    val iTable = "YLZX_OPER_INTEREST"

    val df = mysqlUtil.getMysqlData(spark, url, user, password, iTable).select("INTEREST_ID", "INTEREST_WORD")


    /*
        df.printSchema()
    root
     |-- INTEREST_ID: integer (nullable = false)
     |-- OPERATOR_ID: string (nullable = true)
     |-- INTEREST_WORD: string (nullable = true)
     |-- CREATE_DATE: string (nullable = true)
     */

    val df1 = df.withColumn("word", explode(split($"INTEREST_WORD", ";"))).select("word").dropDuplicates()
    df1.printSchema()
    df1.show(5)
    println(df1.count)

    println(df1.rdd.partitions.size)

    //    df1.repartition(1).write.mode(SaveMode.Overwrite).csv("result/getKeyWords.csv")
    //    df1.write.mode(SaveMode.Overwrite).option("numPartitions", 1).csv("result/getKeyWords.csv")
    df1.coalesce(1).write.mode(SaveMode.Overwrite).csv("result/getKeyWords.csv")

    sc.stop()
    spark.stop()

  }

}
