package com.evayInfo.Inglory.SparkDiary.ml.features

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.{Vector => MLVector, DenseVector => MLDV, SparseVector => MLSV}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/12/24.
 */
object SparseToDenseDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"SparseToDenseDemo1").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val rdd = sc.parallelize(Seq(("1", "今天是圣诞节"),
      ("2","今天天气有点冷"),
      ("3","今天晚上要吃苹果")
    ))
    val df = spark.createDataFrame(rdd).toDF("ID","content")

    // seg words
    def segWordsFunc(content: String): Seq[String] = {
      val seg = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1).toSeq
      val result = seg match {
        case r if (r.length >= 1) => r
        case _ => Seq("") // Seq("null")
      }
      result
    }
    val segWordsUDF = udf((content:String) => segWordsFunc(content))

    val segDF = df.withColumn("segWords", segWordsUDF($"content"))
    val hashingTF = new HashingTF().
      setInputCol("segWords").
      setOutputCol("rawFeatures").
      setNumFeatures(20)

    val featurizedData = hashingTF.transform(segDF)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val tfidfData = idfModel.transform(featurizedData)
    tfidfData.printSchema()
    tfidfData.show(false)

//    val sparseToDenseUDF = udf((sparseVectors:SparseVector)=>(sparseVectors.toDense))
//    val denseDF = tfidfData.withColumn("denseVectors",sparseToDenseUDF($"features"))
//    denseDF.printSchema()
//    denseDF.select("denseVectors").show(false)
//
//    denseDF.select("denseVectors").rdd.map{case Row(denseVector:DenseVector) => MLlibDV(denseVector.toDense)}

    val vector = tfidfData.select("ID","features").rdd.map{
      case Row(id:String, features:MLVector) => (Vectors.fromML(features))
    }
    vector.collect().foreach(println)
    /*
(20,[0,18,19],[0.28768207245178085,0.6931471805599453,0.6931471805599453])
(20,[6,10,11],[0.6931471805599453,0.6931471805599453,0.28768207245178085])
(20,[0,2,11,16],[0.5753641449035617,0.6931471805599453,0.28768207245178085,0.6931471805599453])
     */
    vector.repartition(1).saveAsTextFile("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/SparseVector")

    val DenseVector = tfidfData.select("ID","features").rdd.map{
      case Row(id:String, features:MLVector) => (Vectors.fromML(features).toDense)
    }
    DenseVector.collect().foreach(println)
    /*
[0.28768207245178085,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.6931471805599453,0.6931471805599453]
[0.0,0.0,0.0,0.0,0.0,0.0,0.6931471805599453,0.0,0.0,0.0,0.6931471805599453,0.28768207245178085,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
[0.5753641449035617,0.0,0.6931471805599453,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.28768207245178085,0.0,0.0,0.0,0.0,0.6931471805599453,0.0,0.0,0.0]
     */
    DenseVector.repartition(1).saveAsTextFile("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/DenseVector")



    //
//    val SparseVector = tfidfData.select("ID","features").rdd.map{
//      case Row(id:String, features:MLSV) => (SparseVector.fromML(features))
//    }


    sc.stop()
    spark.stop()

  }

}
