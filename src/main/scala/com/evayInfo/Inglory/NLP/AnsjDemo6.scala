package com.evayInfo.Inglory.NLP

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.MyStaticValue
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/25.
 * AnsjDemo5 RDD版本
 */
object AnsjDemo6 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"AnsjDemo5").//setMaster("local[*]").
      set("spark.executor.memory", "2g").
      set("spark.Kryoserializer.buffer.max", "2048mb")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val rdd = sc.parallelize(Seq(
      ("a", "MMLSpark", "MMLSpark：微软开源的用于Spark的深度学习库"),
      ("b", "数据可视化", "大数据时代当城市数据和社会关系被可视化，每个人都可能是福尔摩斯"),
      ("c", "十九大", "【权威发布】中国共产党第十九届中央委员会候补委员名单")))

    // load stop words
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    // load user defined dic
//    val userDefineFile = "/personal/sunlu/ylzx/userDefine.dic"
//    val userDefineFile = "/personal/sunlu/NLP/userDic_20171024.txt"
    val userDefineFile = "/personal/sunlu/NLP/userDic_20171024.txt"
    val userDefineList = sc.textFile(userDefineFile, 5).collect().toList

    val rdd1 = rdd.map(x => {
      val id = x._1
      val title = x._2
      val content = x._3
      val segContent = title + " " + content

      userDefineList.foreach(x => {
        UserDefineLibrary.insertWord(x, "userDefine", 1000)
      })

//      loadLibrary.loadLibrary("")

//      MyStaticValue.userLibrary = "/root/lulu/Progect/NLP/userDic_20171024.txt"//此方法不可行

      val segWords = ToAnalysis.parse(segContent).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).filter(x => x(1).contains("n") || x(1).contains("userDefine") || x(1).contains("m")).
        map(_ (0)).toList.
        filter(word => word.length >= 2 & !stopwords.contains(word)).mkString(" ")
      (id, segWords)
    })
    rdd1.collect().foreach(println)


    sc.stop()
    spark.stop()
  }

}
