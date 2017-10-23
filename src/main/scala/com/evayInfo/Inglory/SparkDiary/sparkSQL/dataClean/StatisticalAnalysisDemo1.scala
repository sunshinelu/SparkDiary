package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}


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
    import spark.implicits._
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

    // 更改data的数据类型

    val data2 = data.select($"affairs".cast(DoubleType),
      $"gender".cast(StringType),
      $"age".cast(DoubleType),
      $"yearsmarried".cast(DoubleType),
      $"children".cast(StringType),
      $"religiousness".cast(DoubleType),
      $"education".cast(DoubleType),
      $"occupation".cast(DoubleType),
      $"rating".cast(DoubleType))

    data2.show(5, false)
    data2.printSchema()
    /*
    查看数据的统计分布情况
     */
    val descrDF = data2.describe(colArray: _*)
    descrDF.show(5, false)

    descrDF.selectExpr("summary",
      "round(affairs,2) as affairs",
      "round(age,2) as age",
      "round(yearsmarried,2) as yearsmarried",
      "children",
      "round(religiousness,2) as religiousness",
      "round(education,2) as education",
      "round(occupation,2) as occupation",
      "round(rating,2) as rating").show(10, truncate = false)

    /*
    相关系数
     */

    val corr_df = Range(0, 10, step = 1).toDF("id").withColumn("rand1", rand(seed = 10)).withColumn("rand2", rand(seed = 27))
    corr_df.show(false)

    println(corr_df.stat.corr("rand1", "rand2", "pearson"))

    /*
   统计字段中元素的个数
     */

    val fi = data.stat.freqItems(colArray)
    fi.printSchema()
    val f = fi.selectExpr("size(age_freqItems)",
      "size(yearsmarried_freqItems)",
      "size(religiousness_freqItems)",
      "size(education_freqItems)",
      "size(occupation_freqItems)",
      "size(rating_freqItems)")
    f.show(10, truncate = false)

    /*
    集合字段的元素
     */
    val f1 = data.stat.freqItems(Array("age", "yearsmarried", "religiousness"))
    f1.show(10, truncate = false)

    // 对数组的元素排序
    f1.selectExpr("sort_array(age_freqItems)", "sort_array(yearsmarried_freqItems)", "sort_array(religiousness_freqItems)").show(10, truncate = false)

    // 集合字段的元素
    val f2 = data.stat.freqItems(Array("education", "occupation", "rating"))
    f2.show(10, truncate = false)

    // 对数组的元素排序
    f2.selectExpr("sort_array(education_freqItems)", "sort_array(occupation_freqItems)", "sort_array(rating_freqItems)").show(10, truncate = false)

    sc.stop()
    spark.stop()

  }

}
