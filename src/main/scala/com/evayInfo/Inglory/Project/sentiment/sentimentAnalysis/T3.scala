package com.evayInfo.Inglory.Project.sentiment.sentimentAnalysis

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/2/7.
 */
object T3 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  case class Schema(col1:String, col2:Int)
  def main(args: Array[String]) {
    val s = "今天天气很好！"
    val posnegList = List("很","b")
    val words = ToAnalysis.parse(s).toArray.map(_.toString.split("/")).
      filter(_.length >= 1).map(_ (0)).toList.
      filter(word => word.length >= 1 & posnegList.contains(word))
      .toSeq
    println(words.toList)
    println(words.length)
    val t = words.mkString(" ").split(" ")
    t.foreach(println)


    SetLogger

    val conf = new SparkConf().setAppName(s"T3").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val rdd = sc.parallelize(Seq(("a", null),(null,2)))
    val df = spark.createDataFrame(rdd).toDF("col1", "col2")
    df.show(false)
    df.printSchema()
   val df1 = df.na.fill(Map("col2" -> 0))
    df1.show()
    df1.printSchema()




  }

}
