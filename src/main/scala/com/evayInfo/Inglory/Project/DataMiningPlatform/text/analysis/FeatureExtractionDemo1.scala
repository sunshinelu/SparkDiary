package com.evayInfo.Inglory.Project.DataMiningPlatform.text.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{IDFModel, IDF, HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/10/15.
 * 抽取文本特征
 * 词频、IF-ID、Word2Vec
 * 输入：待分析表名、文本所在列(分词后文本，以空格为分隔符)
 * 输出：
 */
object FeatureExtractionDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val SparkConf = new SparkConf().setAppName(s"FeatureExtractionDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show()

    val tf_idf_model_path = ""
    idfModel.save(tf_idf_model_path)
    val idfModel2 = IDFModel.load(tf_idf_model_path)




    sc.stop()
    spark.stop()

  }
}
