package com.evayInfo.Inglory.Project.RenCai.DocSimi


import java.util.Properties

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, MinHashLSH, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * @Author: sunlu
  * @Date: 2019-06-24 13:06
  * @Version 1.0
  */
object DocSimi_Demo1 {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]): Unit = {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"DocSimi_Demo1").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/gongdan?useUnicode=true&characterEncoding=UTF-8"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "rc_jbxx", prop1).
      select("address").na.drop().dropDuplicates().
      filter(length($"address") >= 2).
      withColumnRenamed("address","content").
      withColumn("tag",lit("address"))
    val ds2 = spark.read.jdbc(url1, "district_dict", prop1).
      select("full_name").na.drop().dropDuplicates().
      filter(length($"full_name") >= 2).
      withColumnRenamed("full_name","content").
      withColumn("tag",lit("full_name"))
    val ds  = ds1.union(ds2).filter(length($"content") >= 3)

    /*
using ansj seg words
 */
//    def segWords(txt:String):String = {
//      val wordseg = ToAnalysis.parse(txt)
//      var result = ""
//      for (i <- 0 to wordseg.size() - 1){
//        result = result + " " +  wordseg.get(i).getName()
//      }
//      result
//    }

//    def segWords(txt:String):String = {
//      val segWords = ToAnalysis.parse(txt).toArray().map(_.toString.split("/")).map(_ (0)).toSeq.mkString(" ")
//      val result = if (segWords.length >=3) {
//        segWords
//      } else {
//        " "
//      }
//      result
//    }

    def segWords(txt:String):String = {
      txt.toCharArray.toList.mkString(" ")
    }

    val segWordsUDF = udf((txt: String) => segWords(txt))

    val ds_words = ds.withColumn("seg_words",segWordsUDF($"content")).
      filter(length($"seg_words") >= 3)

    val tokenizer = new Tokenizer().setInputCol("seg_words").setOutputCol("words")
    val wordsData = tokenizer.transform(ds_words)

    /*
 calculate tf-idf value
  */

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20000)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
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
    //    mhTransformed.printSchema()

    val ds_address = mhTransformed.filter($"tag" === "address")
    val ds_full_name = mhTransformed.filter($"tag" === "full_name")


    val docsimi_mh = mhModel.approxSimilarityJoin(ds_address, ds_full_name, 1.0)

    docsimi_mh.printSchema()
    val colRenamed = Seq("address", "full_name","distCol")

    val mhSimiDF = docsimi_mh.select("datasetA.content", "datasetB.content", "distCol").
      toDF(colRenamed: _*).na.drop()

    val w = Window.partitionBy($"address").orderBy($"distCol".asc)
    val ds_simi = mhSimiDF.withColumn("rn", row_number.over(w)).where($"rn" <= 1) //.drop("rn")

    ds_simi.printSchema()
//    ds_simi.show(20,truncate = false)

    ds_simi.coalesce(5).
      write.mode("overwrite").jdbc(url1, "address_simi", prop1) //overwrite  append


    sc.stop()
    spark.stop()
  }
}
