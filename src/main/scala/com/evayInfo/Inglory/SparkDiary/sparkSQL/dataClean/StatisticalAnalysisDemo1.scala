package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/20.
 * 使用spark 进行统计分析
 * 参考链接：
 * Spark ML包，数据挖掘示例数据Affairs： http://www.cnblogs.com/wwxbi/p/6063613.html
 * Spark2 探索性数据统计分析： http://www.cnblogs.com/wwxbi/p/6125363.html
 */
object StatisticalAnalysisDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"StatisticalAnalysisDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    /*
    获取数据
affairs：一年来婚外情的频率
gender：性别
age：年龄
yearsmarried：婚龄
children：是否有小孩
religiousness：宗教信仰程度（5分制，1分表示反对，5分表示非常信仰）
education：学历
occupation：职业（逆向编号的戈登7种分类）
rating：对婚姻的自我评分（5分制，1表示非常不幸福，5表示非常幸福）
     */

    //    val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(


    val colArray: Array[String] = Array("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    val data = spark.read.option("header", false).option("delimiter", ",").
      csv("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/StatisticalAnalysisDate.csv").
      toDF(colArray: _*)
    //    val data = dataList.toDF(colArray:_*)

    data.show(5, false)
    data.printSchema()
    val descrDF = data.describe(colArray: _*)
    descrDF.show(5, false)

    sc.stop()
    spark.stop()

  }

}
