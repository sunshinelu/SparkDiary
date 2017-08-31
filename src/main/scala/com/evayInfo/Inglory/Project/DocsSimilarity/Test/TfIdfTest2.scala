package com.evayInfo.Inglory.Project.DocsSimilarity.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/21.
 */
object TfIdfTest2 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"TfIdfTest").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val documents = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/documents.txt").map(_.split(" ").toSeq).zipWithUniqueId()
    documents.collect().foreach(println)
    /*
    (WrappedArray(today, is, a, good, day!),0)
    (WrappedArray(I, am, a, girl.),2)
    (WrappedArray(today, is, not, a, good, day!),4)
    (WrappedArray(hello, word!),1)

     */

    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)

    val tf = documents.map { x => {
      val tf = hashingTF.transform(x._1)
      (x._2, tf)
    }
    }
    tf.persist()

    //构建idf model
    val idf = new IDF().fit(tf.values)

    //将tf向量转换成tf-idf向量
    val tfidf = tf.mapValues(v => idf.transform(v))

    val tfidf_Indexed = tfidf.map { case (i, v) => new IndexedRow(i, v) }

    val indexed_matrix = new IndexedRowMatrix(tfidf_Indexed)

    val transposed_matrix = indexed_matrix.toCoordinateMatrix.transpose()

    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.5.toDouble
    val upper = 1.0

    val sim = transposed_matrix.toRowMatrix.columnSimilarities(threshhold)
    val sim_threshhold = sim.entries.filter { case MatrixEntry(i, j, u) => u >= threshhold && u <= upper }
    //    sim_threshhold.collect().foreach(println)
    /*
    MatrixEntry(0,4,0.7521233104520024)
     */
    //    println(sim.numCols())
    /*
    5
     */
    //    println(sim.numRows())
    /*
5
 */

    val exact = transposed_matrix.toRowMatrix.columnSimilarities()
    val exactRDD = exact.entries
    exactRDD.collect().foreach(println)
    /*
MatrixEntry(2,3,0.3749789896548412)
MatrixEntry(0,3,0.03537414306758181)
MatrixEntry(0,4,0.897839102815081)
MatrixEntry(3,5,0.07544264360410328)
MatrixEntry(2,4,0.03571515324947806)
MatrixEntry(2,5,0.08483693547311918)
MatrixEntry(3,4,0.2331596985459432)
MatrixEntry(0,2,0.03977901289607115)
     */
    println(exactRDD.count())
    /*
    8
     */

    sc.stop()
    spark.stop()
  }

}
