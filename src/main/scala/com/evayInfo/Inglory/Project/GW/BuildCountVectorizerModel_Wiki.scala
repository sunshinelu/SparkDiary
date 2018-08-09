package com.evayInfo.Inglory.Project.GW

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/8/9.
 */
object BuildCountVectorizerModel_Wiki {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"BuildCountVectorizerModel_Wiki").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val file_path = "file:///root/lulu/Progect/gwdl/zhwiki-segment.txt"
    val colName = Seq("seg_words")
    val df1 = spark.read.textFile(file_path).toDF(colName: _*)

    // 对word_seg中的数据以空格为分隔符转化成seq
    val df2 = df1.withColumn("seg_words_seq", split($"seg_words"," "))
    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer().
      setInputCol("seg_words_seq").
      setOutputCol("features").
      setVocabSize(1024).
      setMinDF(5).
      fit(df2)

    val cvModel_path = "file:///root/lulu/Progect/gwdl/cvModel"
    cvModel.write.overwrite().save(cvModel_path)

    val cvModel_2 = CountVectorizerModel.load(cvModel_path)

    val df3 = cvModel_2.transform(df2)
    df3.show()


    sc.stop()
    spark.stop()

  }

}
