package com.evayInfo.Inglory.SparkDiary.mllib.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/11/1.
 */
object TfIdfDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"TfIdfDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val documents = sc.textFile("file:////Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/documents.txt").
      map(_.split(" ").toSeq)
    val hashingTF = new HashingTF(20)

    val tf = hashingTF.transform(documents)
    tf.cache()

    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    tfidf.collect().foreach(println)
    /*
(20,[1,2,5,8,10],[0.8472978603872037,0.15415067982725836,0.5596157879354227,0.5596157879354227,0.3364722366212129])
(20,[2,9,10,13],[0.15415067982725836,1.252762968495368,0.3364722366212129,0.8472978603872037])
(20,[1,2,5,8,10,18],[0.8472978603872037,0.15415067982725836,0.5596157879354227,0.5596157879354227,0.3364722366212129,0.8472978603872037])
(20,[8,19],[0.5596157879354227,1.252762968495368])
(20,[2,10,13,18],[0.15415067982725836,0.3364722366212129,0.8472978603872037,1.6945957207744073])
(20,[0,2,5,16],[1.252762968495368,0.15415067982725836,0.5596157879354227,1.252762968495368])
    */

    // 获取词对应的下标
    val index_t = hashingTF.indexOf("love")
    println("index of love is: "+ index_t)
    /*
index of love is: 0
 */
    val index_t2 = hashingTF.indexOf(0)
    println("index of 0 is: "+ index_t2)

    tfidf.map(x => {
      val d = x.toDense
      d
    }).collect().foreach(println)
    /*
[0.0,0.8472978603872037,0.15415067982725836,0.0,0.0,0.5596157879354227,0.0,0.0,0.5596157879354227,0.0,0.3364722366212129,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
[0.0,0.0,0.15415067982725836,0.0,0.0,0.0,0.0,0.0,0.0,1.252762968495368,0.3364722366212129,0.0,0.0,0.8472978603872037,0.0,0.0,0.0,0.0,0.0,0.0]
[0.0,0.8472978603872037,0.15415067982725836,0.0,0.0,0.5596157879354227,0.0,0.0,0.5596157879354227,0.0,0.3364722366212129,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.8472978603872037,0.0]
[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.5596157879354227,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.252762968495368]
[0.0,0.0,0.15415067982725836,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.3364722366212129,0.0,0.0,0.8472978603872037,0.0,0.0,0.0,0.0,1.6945957207744073,0.0]
[1.252762968495368,0.0,0.15415067982725836,0.0,0.0,0.5596157879354227,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.252762968495368,0.0,0.0,0.0]

     */


    sc.stop()
    spark.stop()
  }
}
