package com.evayInfo.Inglory.Project.ldaModel.Test

import com.evayInfo.Inglory.Project.ldaModel.LdaUtil
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}

import scala.collection.Map


/**
  * Created by sunlu on 17/8/26.
  */

object ldaDemoDF {
  def main(args: Array[String]): Unit = {
    LdaUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"ldaDemoDF").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Loads data.
    val dataset = spark.read.format("libsvm")
      .load("file:///D:\\Workspace\\IDEA\\GitHub\\SparkDiary\\data\\sample_lda_libsvm_data.txt")
   // Trains a LDA model.
    val lda = new LDA().setK(5).setMaxIter(10)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)
    transformed.printSchema()

    val document = transformed.select("label","topicDistribution").na.drop.rdd.map {
      case Row(id:Double, features: MLVector) =>  (id.toLong, Vectors.fromML(features))}

    val inedxed = document.map { case (i, v) => new IndexedRow(i, v) }
    val mat = new IndexedRowMatrix(inedxed)
    val mat_t = mat.toCoordinateMatrix.transpose()
    val sim = mat_t.toRowMatrix.columnSimilarities()
    sim.entries.collect().foreach(println)


    val k = 100
    val svd = mat.computeSVD(k, computeU=true)



    sc.stop()
    spark.stop()
  }
}
