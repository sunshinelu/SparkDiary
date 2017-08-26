package com.evayInfo.Inglory.Project.DocsSimilarity

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

object DocsimiTfIdf {
  def main(args: Array[String]): Unit = {

    DocsimiUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiTfIdf").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable =  "yilan-total_webpage"
    val docSimiTable = "docsimi_svd"

    val ylzxRDD = DocsimiCountVectorizer.getYlzxRDD(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).randomSplit(Array(0.01, 0.99))(0)

    println("ylzxDS的数量为：" + ylzxDS.count())

    val vocabSize: Int = 20000

    val vocabModel: CountVectorizerModel = new CountVectorizer().
      setInputCol("segWords").
      setOutputCol("features").
      setVocabSize(vocabSize).
      setMinDF(2).
      fit(ylzxDS)



  }
}
