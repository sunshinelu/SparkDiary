package com.evayInfo.Inglory.Project.shijijinbang

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{MinHashLSH, IDF, HashingTF}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/7/24.
 * calculate document similarity
 */
class DocSimi extends Serializable{

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  /*
  using ansj seg words
   */
  def segWords(txt:String):String = {
    val segWords = ToAnalysis.parse(txt).toArray().map(_.toString.split("/")).map(_ (0)).toSeq.mkString(" ")
    segWords
  }

  def DocSimiJaccard(docList: List[(Int, String)]): List[(Int, Int, Double)] = {
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("DocSimiJaccard").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // load list and turn list to RDD
    val rdd1 = sc.parallelize(docList).map(x => (x._1, x._2))

    // convert rdd to dataframe
    val colName = Seq("id","txt")
    val df1 = spark.createDataset(rdd1).toDF(colName: _*)

    // segWords
    val segWordsUDF = udf((txt: String) => segWords(txt))
    val df2 = df1.withColumn("seg_words",segWordsUDF($"txt"))

    // 对word_seg中的数据以空格为分隔符转化成seq
    val df3 = df2.withColumn("seg_words_seq", split($"seg_words"," ")).drop("txt").drop("seg_words")
    /*
  calculate tf-idf value
   */
    val hashingTF = new HashingTF().
      setInputCol("seg_words_seq").
      setOutputCol("rawFeatures") //.setNumFeatures(20000)

    val featurizedData = hashingTF.transform(df3)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    // calculate tf-idf valut
    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val tfidfData = idfModel.transform(featurizedData)

    /*
using Jaccard Distance calculate doc-doc similarity
    */
    val mh = new MinHashLSH().
      setNumHashTables(3).
      setInputCol("features").
      setOutputCol("mhValues")
    val mhModel = mh.fit(tfidfData)

    // Feature Transformation
    val mhTransformed = mhModel.transform(tfidfData)
    val docsimi_mh = mhModel.approxSimilarityJoin(mhTransformed, mhTransformed, 1.0)

    // select columns
    val colRenamed = Seq("doc1Id", "doc1", "doc2Id", "doc2","distCol")

    val mhSimiDF = docsimi_mh.select("datasetA.id", "datasetA.seg_words_seq", "datasetB.id", "datasetB.seg_words_seq","distCol").
      toDF(colRenamed: _*).filter($"doc1Id" =!= $"doc2Id")

    //    mhSimiDF.show(200)
    val result_df = mhSimiDF.select("doc1Id","doc2Id","distCol").
      withColumn("distCol", bround($"distCol", 3)).
      filter($"doc1Id" < $"doc2Id").orderBy($"doc1Id".asc,$"distCol".asc)

    val result_list = result_df.rdd.map{case Row(doc1Id:Int, doc2Id:Int,docSimi:Double) => (doc1Id, doc2Id, docSimi)}.collect().toList
    sc.stop()
    spark.stop()
    return result_list

  }

}
