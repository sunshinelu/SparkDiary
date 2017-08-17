package com.evayInfo.Inglory.Project.DocsSimilarity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by sunlu on 17/8/17.
 */
object synonymsWords {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {


    SetLogger

    val conf = new SparkConf().setAppName(s"synonymsWords").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val word2VecModel = Word2VecModel.load("/Users/sunlu/Desktop/Word2VecModelDF_dic")
    val keyWordsDF = spark.read.format("text").load("library/userDefine.dic")
    //    keyWordsDF.show()


    val keyWordsRDD = sc.textFile("library/userDefine.dic").collect().toList

    //    keyWordsRDD

    /*
        val keyWords = Vector("科技", "人才")
        val synonymsRDD = sc.parallelize(keyWordsRDD).map { x =>
          (x, try {
            word2VecModel.findSynonyms(x, 100).map(_._1).mkString(";")
          } catch {
            case e: Exception => ""
          })
        }
        synonymsRDD.take(5).foreach(println)

        synonymsRDD.saveAsTextFile("result/synonymsRDD")

    */

    /*
        val word2VecFunc = udf((keyWords: String, num: Int) => {
          word2VecModel.findSynonyms(keyWords, num)
        })
        val t1 = keyWords.withColumn("synonymes", word2VecFunc(col("keywords")))

        t1.show()

        val keyWords1 = org.apache.spark.ml.linalg.Vectors(Vector("科技", "人才")
        word2VecModel.findSynonyms("科技", 2).collect().foreach(println)


        word2VecModel.findSynonyms(keyWords1, 4)

        keyWords1
    */

    val t1 = word2VecModel.getVectors
    t1.printSchema()
    println(t1.count())
    t1.take(5).foreach(println)

    // save vocabulary data
    t1.select("word").coalesce(1).write.mode(SaveMode.Overwrite).csv("result/vocabulary.csv")

    /*
    for (keyWord <- keyWordsRDD) {
     val df1 = word2VecModel.findSynonyms(keyWord, 100).toDF("synonyms","score").withColumn("keyword", lit(keyWord))
      df1.coalesce(1).write.mode(SaveMode.Append).csv("result/getSynonyms.csv")
    }
*/


    /*
    Exception in thread "main" java.lang.IllegalStateException: 双创 not in vocabulary
    战略资讯、大数据、IT
     */
    sc.stop()
    spark.stop()
  }

}
