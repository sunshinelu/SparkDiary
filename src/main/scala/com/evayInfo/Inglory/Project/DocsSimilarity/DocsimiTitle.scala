package com.evayInfo.Inglory.Project.DocsSimilarity

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, HashingTF, IDF, MinHashLSH}
import org.apache.spark.sql.SparkSession

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

    val ylzxTable = args(0)
    val docSimiTable = args(1)
    /*
    val ylzxTable =  "yilan-total_webpage"
    val docSimiTable = "docsimi_title"
*/

    val ylzxRDD = DocsimiUtil.getYlzxSegRDD(ylzxTable, 20, sc)
    val ylzxDS = spark.createDataset(ylzxRDD) //.randomSplit(Array(0.01, 0.99))(0)

    val hashingTF = new HashingTF()
      .setInputCol("segWords").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(ylzxDS)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("id", "features").show()

    /*

     */
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("brpValues")
    val brpModel = brp.fit(rescaledData)

    val brpTransformed = brpModel.transform(rescaledData).cache()
    val docsimi_brp = brpModel.approxSimilarityJoin(brpTransformed, brpTransformed, 5.0)

    /*

     */
    val mh = new MinHashLSH()
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")
    val mhModel = mh.fit(rescaledData)

    // Feature Transformation
    val mhTransformed = mhModel.transform(rescaledData)
    val docsimi_mh = mhModel.approxSimilarityJoin(mhTransformed, mhTransformed, 1.0)




    sc.stop()
    spark.stop()
  }

}
