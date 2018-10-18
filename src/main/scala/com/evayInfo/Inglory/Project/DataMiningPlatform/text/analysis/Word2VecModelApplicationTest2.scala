package com.evayInfo.Inglory.Project.DataMiningPlatform.text.analysis

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.feature.{Word2VecModel, Word2Vec, Tokenizer}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/10/18.
 */
object Word2VecModelApplicationTest2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {

    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)

    val SparkConf = new SparkConf().setAppName(s"BuildFeatureExtractionModel:WordCount").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ipt_train_table = "Sogou_Classification_mini_segWords_random100"
    val opt_train_table_WordCount: String = "feature_extraction_Word2Vec"
    val ipt_test_table = "Sogou_Classification_mini_segWords_random10"
    val opt_test_table_WordCount: String = "feature_extraction_Word2Vec_test"
    val colName = "seg_words"
    val feature_size = 20
    val min_count = 2
    val model_path: String = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/data_mining_platform/feature_extraction_model_word2vec"

    // 读取mysql数据
    // get ipt_df
    val ipt_train_df = spark.read.jdbc(url, ipt_train_table, prop)
    val ipt_test_df = spark.read.jdbc(url, ipt_test_table, prop)


    // Split the text into Array
    val tokenizer_df_train = new Tokenizer().
      setInputCol(colName).
      setOutputCol("words").
      transform(ipt_train_df)

    val tokenizer_df_test = new Tokenizer().
      setInputCol(colName).
      setOutputCol("words").
      transform(ipt_test_df)

    // Word2Vec
    val word2vec_model = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("features")
      .setVectorSize(feature_size)
      .setMinCount(min_count).fit(tokenizer_df_train)

    // save cv_model model
    word2vec_model.write.overwrite().save(model_path)

    // Predict on the sentenceData dataset
    val df_Word2Vec = word2vec_model.transform(tokenizer_df_train)
    //    df_Word2Vec.show(truncate = false)
    //将结果保存到数据框中
    // 列features的array类型转成string类型，因为mysql中没有array类型
    val MLVectorToString = udf((features:MLVector) => Vectors.fromML(features).toDense.toString())

    val results_train_df = df_Word2Vec.withColumn("features", MLVectorToString($"features")).drop("words")

    results_train_df.write.mode("overwrite").jdbc(url, opt_train_table_WordCount, prop) //overwrite ; append


    val load_word2vec_model = Word2VecModel.load(model_path)


    val df_Word2Vec_test = load_word2vec_model.transform(tokenizer_df_test)
    //将结果保存到数据框中

    val results_test_df = df_Word2Vec.withColumn("features", MLVectorToString($"features")).drop("words")

    results_test_df.write.mode("overwrite").jdbc(url, opt_test_table_WordCount, prop) //overwrite ; append
    sc.stop()
    spark.stop()


  }

}
