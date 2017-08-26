package com.evayInfo.Inglory.Project.ldaModel.Test

import com.evayInfo.Inglory.Project.ldaModel.LdaUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.linalg.{Vector => MLVector}

/**
  * Created by sunlu on 17/8/26.
  */

object ldaDemoRDD {

  def main(args: Array[String]): Unit = {
    LdaUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"ldaDemoRDD").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Load and parse the data
    val data = sc.textFile("file:///D:\\Workspace\\IDEA\\GitHub\\SparkDiary\\data\\sample_lda_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(" " + topics(word, topic));
      }
      println()
    }

    val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
//    val t1 = distLDAModel.topTopicsPerDocument(3).map{ case (i:Int, v: MLVector) => new IndexedRow(i, v) }
//    println("t1 is: " + t1)
//    t1.collect().foreach(println)

    // Save and load model.
    //    ldaModel.save(sc, "target/org/apache/spark/LatentDirichletAllocationExample/LDAModel")
    //    val sameModel = DistributedLDAModel.load(sc,
    //      "target/org/apache/spark/LatentDirichletAllocationExample/LDAModel")


    sc.stop()
    spark.stop()

  }

}
