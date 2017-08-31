package com.evayInfo.Inglory.Project.DocsSimilarity

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, HashingTF, IDF, MinHashLSH}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{column, udf}

/**
  * Created by sunlu on 17/8/31.
  *
  * 使用文章标题计算文章相似性
  *
  */
object DocsimiTitle {
  def main(args: Array[String]) {
    DocsimiUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiTitle").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
        val ylzxTable = args(0)
    val docSimiTable = args(1)
     */


    val ylzxTable = "yilan-total_webpage"
    val docSimiTable = "docsimi_title"


    val ylzxRDD = DocsimiUtil.getYlzxSegRDD(ylzxTable, 20, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).drop("segWords") //.randomSplit(Array(0.01, 0.99))(0)

    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    //定义UDF
    //分词、停用词过滤
    def segWordsFunc(content: String): Seq[String] = {
      val seg = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.value.contains(word)).toSeq
      val result = seg match {
        case r if (r.length >= 1) => r
        case _ => Seq("")// Seq("null")
      }
      result
    }

    val segWordsUDF = udf((content: String) => segWordsFunc(content))

    val segDF = ylzxDS.withColumn("segWords", segWordsUDF(column("title")))//.filter(!$"segWords".contains("null"))

    /*
    calculate tf-idf value
     */
    val hashingTF = new HashingTF()
      .setInputCol("segWords").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(segDF)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("id", "features").show()

    /*
using  Euclidean Distance calculate doc-doc similarity
     */
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("brpValues")
    val brpModel = brp.fit(rescaledData)

    val brpTransformed = brpModel.transform(rescaledData).cache()
    val docsimi_brp = brpModel.approxSimilarityJoin(brpTransformed, brpTransformed, 5.0)
    docsimi_brp.printSchema()

    /*
using Jaccard Distance calculate doc-doc similarity
     */
    val mh = new MinHashLSH()
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")
    val mhModel = mh.fit(rescaledData)

    // Feature Transformation
    val mhTransformed = mhModel.transform(rescaledData)
    val docsimi_mh = mhModel.approxSimilarityJoin(mhTransformed, mhTransformed, 1.0)
    docsimi_mh.printSchema()


    sc.stop()
    spark.stop()
  }

}
