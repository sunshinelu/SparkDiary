package com.evayInfo.Inglory.SparkDiary.mllib.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/11/1.
 * 使用RDD计算tfidf，然后从稀疏矩阵中提取每个词对用的权重
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
    /*
    index of 0 is: 11
     */

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

    //提取出作为矩阵的关键词
    val mapWords = documents.flatMap(x => x).map(w => (hashingTF.indexOf(w),w)).collect.toMap
    mapWords.foreach(println)
    /*
(0,love)
(5,you)
(10,a)
(1,is)
(9,girl)
(13,am)
(2,I)
(18,boy)
(16,mom)
(8,hello)
(19,word)
     */
    val bcWords = tf.context.broadcast(mapWords)
    val token = tfidf.map{
      case SV(size, indices, values) =>
        val words = indices.map(index => bcWords.value.getOrElse(index,"null"))
        words.zip(values).sortBy(-_._2)//.take(nWord).map(x=>x._1)
          .toSeq
    }.flatMap(x => x).reduceByKey(_ + _).sortBy(-_._2)//.collect//.distinct
    println("提取出作为矩阵的关键词:")
    token.foreach(println)
    /*
(girl,1.252762968495368)
(boy,2.5418935811616112)
(love,1.252762968495368)
(is,1.6945957207744073)
(word,1.252762968495368)
(am,1.6945957207744073)
(mom,1.252762968495368)
(I,0.7707533991362918)
(hello,1.678847363806268)
(you,1.678847363806268)
(a,1.3458889464848516)
     */

    sc.stop()
    spark.stop()
  }
}
