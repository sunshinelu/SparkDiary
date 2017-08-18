package com.evayInfo.Inglory.Project.DocsSimilarity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/18.
 * 获取ml.feature.Word2VecModel方法生成的word2vec
 * 任务运行报错：
 * org.json4s.package$MappingException: Did not find value which can be converted into java.lang.String
 *
 */
object synonymsWordsRDD {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {


    SetLogger

    val conf = new SparkConf().setAppName(s"synonymsWords").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val word2VecModel = Word2VecModel.load(sc, "/Users/sunlu/Desktop/Word2VecModelDF_dic")
    val keyWordsDF = spark.read.format("text").load("library/userDefine.dic")
    //    keyWordsDF.show()


    val keyWordsRDD = sc.textFile("library/userDefine.dic").collect().toList

    //    keyWordsRDD


    val keyWords = Vector("科技", "人才")
    val synonymsRDD = sc.parallelize(keyWordsRDD).map { x =>
      (x, try {
        word2VecModel.findSynonyms(x, 100).map(_._1).mkString(";")
      } catch {
        case e: Exception => ""
      })
    }
    synonymsRDD.take(5).foreach(println)

    synonymsRDD.saveAsTextFile("result/synonymsRDD")

    /*
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Exception in thread "main" org.json4s.package$MappingException: Did not find value which can be converted into java.lang.String
      at org.json4s.Extraction$.convert(Extraction.scala:603)
      at org.json4s.Extraction$.extract(Extraction.scala:350)
      at org.json4s.Extraction$.extract(Extraction.scala:42)
      at org.json4s.ExtractableJsonAstNode.extract(ExtractableJsonAstNode.scala:21)
      at org.apache.spark.mllib.util.Loader$.loadMetadata(modelSaveLoad.scala:131)
      at org.apache.spark.mllib.feature.Word2VecModel$.load(Word2Vec.scala:679)
      at com.evayInfo.Inglory.Project.DocsSimilarity.synonymsWordsRDD$.main(synonymsWordsRDD.scala:34)
      at com.evayInfo.Inglory.Project.DocsSimilarity.synonymsWordsRDD.main(synonymsWordsRDD.scala)
      at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
      at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
      at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
      at java.lang.reflect.Method.invoke(Method.java:497)
      at com.intellij.rt.execution.application.AppMain.main(AppMain.java:140)
     */


    sc.stop()
    spark.stop()
  }
}
