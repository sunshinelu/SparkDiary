package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/26.
 */
object tfidfVsCountVectorizer {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"tfidfVsCountVectorizer").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
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
      .setInputCol("words").setOutputCol("tfFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("tfFeatures").setOutputCol("tfidfFeatures")
    val idfModel = idf.fit(featurizedData)

    val tfidfData = idfModel.transform(featurizedData)
    tfidfData.show(false)

    idfModel.idf.foreachActive((index,value)=>println(index+" "+value+" "))

    val tfidfValue = idfModel.idf//.foreachActive((index,value)=>(index,value+" "))
//    sc.parallelize(tfidfValue)



    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("cvFeatures")
      .setVocabSize(20)
      .setMinDF(1)
      .fit(wordsData)

    val cvData = cvModel.transform(wordsData)
    cvData.show(false)
    val cvVocabulary = cvModel.vocabulary.mkString(";")
    println(cvVocabulary)


    sc.stop()
    spark.stop()

  }

}
