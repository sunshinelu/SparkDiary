package com.evayInfo.Inglory.Project.DocsSimilarity

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

/**
 * Created by sunlu on 17/8/14.
 */
object word2VecSimiTest2 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger
    val conf = new SparkConf().setAppName(s"word2VecSimiTest1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val begin = dateFormat.format(new Date())
    val beginL = dateFormat.parse(begin).getTime
    println(begin)
    println(beginL)


    // reload word2vec Model
    val word2VecModel = Word2VecModel.load("result/word2vevModel")
    word2VecModel.findSynonyms("科技", 2).collect().foreach(println)


    val df1 = sc.parallelize(Seq(("科技"), ("人才"))).toDF("keyWords")
    df1.show()

    val word2VecFunc = udf((keyWords: String, num: Int) => {
      word2VecModel.findSynonyms(keyWords, num)
    })

    val df2 = df1.withColumn("synonymes", word2VecFunc(col("keyWords")))
    df2.show()



    val end = dateFormat.format(new Date())
    val endL = dateFormat.parse(end).getTime
    val between: Long = (endL - beginL) / 1000 //转化成秒
    val hour: Float = between.toFloat / 3600
    val decf: DecimalFormat = new DecimalFormat("#.00")
    println(decf.format(hour)) //格式化
    println("任务运行时间为：" + between + "秒")
    println("任务运行时间为：" + hour + "小时")

  }


}
