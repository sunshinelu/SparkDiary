package com.evayInfo.Inglory.Project.GW

import java.util.{Properties, UUID}

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{MinHashLSH, CountVectorizerModel}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors => MLVectors}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

/**
 * Created by sunlu on 18/8/9.
 */
class zhptUtil {

  // do not print log
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
    val wordseg = ToAnalysis.parse(txt)
    var result = ""
    for (i <- 0 to wordseg.size() - 1){
      result = result + " " +  wordseg.get(i).getName()
    }
    result
  }

  /*
  输入：ID和文本的List
  输入：ID、文本、特征的List
   */
  def ExtractFeatures(StrList:java.util.List[(String, String)],
                      url:String, user:String,password:String,mysql_Table:String,
                      cvModel_path:String) :java.util.List[(String, String,String)] = {

    SetLogger
    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"ExtractFeatures").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // load list and turn list to RDD
    val df1 = sc.parallelize(StrList.asScala).map(x => (x._1, x._2)).toDF("id","txt")
    val segWordsUDF = udf((txt: String) => segWords(txt))

    val df2 = df1.withColumn("seg_words",segWordsUDF($"txt"))
    // 对word_seg中的数据以空格为分隔符转化成seq
    val df3 = df2.withColumn("seg_words_seq", split($"seg_words"," ")).drop("seg_words")

    // load CountVectorizerModel
    val cvModel = CountVectorizerModel.load(cvModel_path)

    // extract features
    val df4 = cvModel.transform(df3).select("id","txt","features")

    val prop = new Properties()
    prop.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop.setProperty("user", user)
    prop.setProperty("password", password)


    val resultList = df4.rdd.map {
      case Row(id: String, txt:String,features: MLVector) => (id, txt, Vectors.fromML(features).toDense.toString())
    }.collect().toList.asJava

    sc.stop()
    spark.stop()
    return resultList
  }

  /*
  输入：文本字符串
  输入：ID和相似性打分
   */
  def DocSimi(iptStr:String,url:String, user:String,password:String,
              mysql_Table:String,cvModel_path:String): java.util.List[(String, Double)] = {

    SetLogger
    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"ExtractFeatures").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    val spark = SparkSession.builder.appName("DocSimiJaccard").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    */

    val id_1 = UUID.randomUUID().toString().toLowerCase()
    val df1 = sc.parallelize(Seq((id_1,iptStr))).toDF("id","txt")

    val segWordsUDF = udf((txt: String) => segWords(txt))

    val df2 = df1.withColumn("seg_words",segWordsUDF($"txt"))
    // 对word_seg中的数据以空格为分隔符转化成seq
    val df3 = df2.withColumn("seg_words_seq", split($"seg_words"," ")).drop("txt").drop("seg_words")
    // load CountVectorizerModel
    val cvModel = CountVectorizerModel.load(cvModel_path)

    val df4 = cvModel.transform(df3).select("id","features")

    val prop = new Properties()
    prop.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop.setProperty("user", user)
    prop.setProperty("password", password)


    val features_df = spark.read.jdbc(url, mysql_Table, prop).rdd.map { case Row(id: String, features: String) =>
      val features_vector = features.replace("[", "").replace("]", "").split(",").
        toSeq.toArray.map(_.toDouble)
      val features_dense_vector = MLVectors.dense(features_vector).toSparse
      (id, features_dense_vector)
    }.toDF("id", "features")

    val mh = new MinHashLSH().
      setNumHashTables(3).
      setInputCol("features").
      setOutputCol("mhValues")
    val mhModel = mh.fit(df2)

    // Feature Transformation
    val mhTransformed = mhModel.transform(df2)
    val docsimi_mh = mhModel.approxSimilarityJoin(df4, features_df, 1.0)

    val colRenamed = Seq("doc1Id", "doc2Id","distCol")
    val mhSimiDF = docsimi_mh.select("datasetA.id", "datasetB.id","distCol").
      withColumn("distCol", bround($"distCol", 3)).
      toDF(colRenamed: _*).orderBy($"doc1Id".asc,$"distCol".asc)


    val result_list = mhSimiDF.rdd.map{case Row(doc1Id:String, doc2Id:String,distCol:Double) =>
      (doc2Id, distCol)}.collect().toList.asJava

    sc.stop()
    spark.stop()
    return result_list
  }

}
