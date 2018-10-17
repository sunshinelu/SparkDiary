package com.evayInfo.Inglory.Project.DataMiningPlatform.text.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/10/15.
 * 使用Pipeline参考链接：
 *
 * https://github.com/hupi-analytics/Pipeline-SentimentAnalysis/blob/master/Pipeline%20example.scala
 */
object FeatureExtractionDemo2 {

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

    val test_df = spark.createDataFrame(Seq(
      (0.0, "今天 天气 很 好 ！"),
      (0.0, "不 想 去 国网 。 "),
      (1.0, "明天 会 下雨 吗 ？")
    )).toDF("label", "sentence")

    // Split the text into Array
    val tokenizer = new Tokenizer().
      setInputCol("sentence").
      setOutputCol("words")

    // CountVectorizer
     val countVectorizer = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(10) // 若不设置该参数则默认使用全部词的数量，在此例中是词的数量是16
      .setMinDF(1)


    // TF-IDF
    val hashingTF = new HashingTF().
      setInputCol(tokenizer.getOutputCol).
      setOutputCol("rawFeatures")

    val idf = new IDF().
      setInputCol(hashingTF.getOutputCol).
      setOutputCol("features")

    // Word2Vec
    val word2Vec = new Word2Vec()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
      .setVectorSize(5)
      .setMinCount(0)

    // Create our pipeline
    val cv_pipeline = new Pipeline().setStages(Array(tokenizer, countVectorizer))
    val ifidf_pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf))
    val word2vec_pipeline = new Pipeline().setStages(Array(tokenizer, word2Vec))

    // Train the model
    val cv_model = cv_pipeline.fit(sentenceData)
    val ifidf_model = ifidf_pipeline.fit(sentenceData)
    val word2vec_model = word2vec_pipeline.fit(sentenceData)

    // Predict on the sentenceData dataset
    val sentenceData_TFIDF = ifidf_model.transform(sentenceData).select('label, 'sentence, 'features)
    sentenceData_TFIDF.show(truncate = false)

    val sentenceData_CV = cv_model.transform(sentenceData).select('label, 'sentence, 'features)
    sentenceData_CV.show(truncate = false)

    val sentenceData_word2vec = word2vec_model.transform(sentenceData).select('label, 'sentence, 'features)
    sentenceData_word2vec.show(truncate = false)

    // Predict on the test dataset
    val test_pred = ifidf_model.transform(test_df).select('label, 'sentence, 'features)
    test_pred.show(truncate = false)

//    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
//    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
//    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")



    sc.stop()
    spark.stop()
  }

}
