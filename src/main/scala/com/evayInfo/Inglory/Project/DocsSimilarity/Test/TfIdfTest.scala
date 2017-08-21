package com.evayInfo.Inglory.Project.DocsSimilarity.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/21.
 */
object TfIdfTest {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName(s"TfIdfTest").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val documents = sc.textFile("data/documents.txt").map(_.split(" ").toSeq)
    val hashingTF = new HashingTF()

    val tf = hashingTF.transform(documents)
    tf.cache()

    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    //    tfidf.collect().foreach(println)
    /*
    (1048576,[99189,406038,489554,540177,899864],[0.5108256237659907,0.5108256237659907,0.22314355131420976,0.5108256237659907,0.5108256237659907])
    (1048576,[166629,376034,489554,686055],[0.9162907318741551,0.9162907318741551,0.22314355131420976,0.9162907318741551])
    (1048576,[99189,406038,489554,540177,663386,899864],[0.5108256237659907,0.5108256237659907,0.22314355131420976,0.5108256237659907,0.9162907318741551,0.5108256237659907])
    (1048576,[165160,901196],[0.9162907318741551,0.9162907318741551])
     */
    // now use the RowMatrix to compute cosineSimilarities
    // which implements DIMSUM algorithm

    val mat = new RowMatrix(tfidf)
    val sim = mat.columnSimilarities() //.transpose()
    val transformedRDD = sim.entries.map { case MatrixEntry(row: Long, col: Long, sim: Double) => Array(row, col, sim).mkString(",") }

    //    transformedRDD.collect().foreach(println)
    /*
    166629.0,489554.0,0.5773502691896257
    406038.0,540177.0,1.0000000000000002
    406038.0,489554.0,0.816496580927726
    406038.0,663386.0,0.7071067811865476
    663386.0,899864.0,0.7071067811865476
    489554.0,899864.0,0.816496580927726
    489554.0,663386.0,0.5773502691896257
    99189.0,489554.0,0.816496580927726
    540177.0,663386.0,0.7071067811865476
    165160.0,901196.0,1.0
    376034.0,686055.0,1.0
    540177.0,899864.0,1.0000000000000002
    489554.0,540177.0,0.816496580927726
    99189.0,540177.0,1.0000000000000002
    99189.0,899864.0,1.0000000000000002
    489554.0,686055.0,0.5773502691896257
    166629.0,686055.0,1.0
    406038.0,899864.0,1.0000000000000002
    99189.0,663386.0,0.7071067811865476
    166629.0,376034.0,1.0
    99189.0,406038.0,1.0000000000000002
    376034.0,489554.0,0.5773502691896257
     */
    sc.stop()
    spark.stop()
  }
}
