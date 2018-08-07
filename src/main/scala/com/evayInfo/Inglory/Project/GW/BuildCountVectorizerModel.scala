package com.evayInfo.Inglory.Project.GW

import java.util.Properties

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizerModel, CountVectorizer}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/8/7.
 */
object BuildCountVectorizerModel {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"BuildCountVectorizerModel").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val file_path = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/shijijinbang/self_simi_test1.txt"
    val colName = Seq("txt")
    val df1 = spark.read.textFile(file_path).toDF(colName: _*)

    // 新增一列递增列
    val w1 = Window.orderBy("txt")
    val df2 = df1.withColumn("id", row_number().over(w1))
    df2.show(false)

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
    val segWordsUDF = udf((txt: String) => segWords(txt))

    val df3 = df2.withColumn("seg_words",segWordsUDF($"txt"))
    // 对word_seg中的数据以空格为分隔符转化成seq
    val df4 = df3.withColumn("seg_words_seq", split($"seg_words"," ")).drop("txt").drop("seg_words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("seg_words_seq")
      .setOutputCol("features")
      .setVocabSize(1024)
      .setMinDF(2)
      .fit(df4)

    val cvModel_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/cvModel"
    cvModel.write.overwrite().save(cvModel_path)

    val cvModel_2 = CountVectorizerModel.load(cvModel_path)

    val df5 = cvModel_2.transform(df4)
    df5.show()

    /*
    df5.select($"id",$"features".cast("array")).coalesce(1).
      write.option("delimiter", "#").mode(SaveMode.Append).
      csv("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/features")
*/

    /*
    df5.select("id","features").coalesce(1).
      write.format("csv").option("delimiter", "#").mode(SaveMode.Append).
      save("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/features")
      */

    df5.select($"id".cast("string"),$"features").rdd.map {
      case Row(id: String, features: MLVector) => (id, Vectors.fromML(features).toDense.toString())
    }.toDF("id","features").coalesce(1).
      write.option("delimiter", "#").mode(SaveMode.Append).
      csv("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/features")


    val url = "jdbc:mysql://localhost:3306/gwdl?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    val mysql_Table = "features"
    df5.select($"id".cast("string"),$"features").rdd.map {
      case Row(id: String, features: MLVector) => (id, Vectors.fromML(features).toDense.toString())
    }.toDF("id","features").coalesce(1).
      write.mode(SaveMode.Append).jdbc(url, mysql_Table, prop)


    sc.stop()
    spark.stop()
  }

}
