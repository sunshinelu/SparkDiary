package com.evayInfo.Inglory.Project.shijijinbang

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, IDF, HashingTF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/7/23.
 */
object SelfSimiEuclidean {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("SelfSimi").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val file_path = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/shijijinbang/self_simi_test1.txt"
    val colName = Seq("txt")
    val df1 = spark.read.textFile(file_path).toDF(colName: _*)
    //    df1.show()

    // 新增一列递增列
    val w1 = Window.orderBy("txt")
    val df2 = df1.withColumn("id", row_number().over(w1))
    df2.show(false)

    def segWords(txt:String):String = {
      val segWords = ToAnalysis.parse(txt).toArray().map(_.toString.split("/")).map(_ (0)).toSeq.mkString(" ")
      segWords
    }
    val segWordsUDF = udf((txt: String) => segWords(txt))

    val df3 = df2.withColumn("seg_words",segWordsUDF($"txt"))
    //    df3.show()

    // 对word_seg中的数据以空格为分隔符转化成seq
    val df4 = df3.withColumn("seg_words_seq", split($"seg_words"," ")).drop("txt").drop("seg_words")

    /*
   calculate tf-idf value
    */
    val hashingTF = new HashingTF().
      setInputCol("seg_words_seq").
      setOutputCol("rawFeatures") //.setNumFeatures(20000)

    val featurizedData = hashingTF.transform(df4)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val tfidfData = idfModel.transform(featurizedData)

    /*
    using Euclidean Distance calculate doc-doc similarity
     */

    val brp = new BucketedRandomProjectionLSH().
      setBucketLength(2.0).
      setNumHashTables(3).
      setInputCol("features").
      setOutputCol("brpVec")

    val brpModel = brp.fit(tfidfData)
    // Feature Transformation
    val brpTransformed = brpModel.transform(tfidfData)
    val simiDF = brpModel.approxSimilarityJoin(brpTransformed, brpTransformed, 5)

    val colRenamed1 = Seq("doc1Id", "doc2Id", "distCol")
    val simiDF2 = simiDF.select("datasetA.id", "datasetB.id", "distCol").toDF(colRenamed1: _*).
      filter($"doc1Id" < $"doc2Id")

    simiDF2.show(200)


    sc.stop()
    spark.stop()

  }

}
