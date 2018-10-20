package com.evayInfo.Inglory.Project.DataMiningPlatform.text.analysis

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/10/17.
 * 功能描述：使用已经构建好的模型生成特征
 * 注意：使用的输入数据的列名需要与构建的模型的列名一致，否则会报错。
 * 输入：模型路径、表名、特征提取后的表名
 * 输出：特征提取后的表
 *
 */
class FeatureExtractionModelApplication {

  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  // 链接mysql配置信息
  val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
    "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
  val user = "root"
  val password = "root"
  val prop = new Properties()
  prop.setProperty("user", user)
  prop.setProperty("password", password)


  def FeatureExtractionModel_TF_IDF(model_path:String, ipt_table:String, opt_table:String) = {
    val SparkConf = new SparkConf().setAppName(s"FeatureExtractionModelApplication:TF_IDF").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val ipt_df = spark.read.jdbc(url, ipt_table, prop)

    // 加载模型
    val ifidf_model = PipelineModel.load(model_path)


    // Predict on the sentenceData dataset
    val df_TFIDF = ifidf_model.transform(ipt_df)

    //将结果保存到数据框中
    // 列features的array类型转成string类型，因为mysql中没有array类型
    val MLVectorToString = udf((features:MLVector) => Vectors.fromML(features).toDense.toString())

    val results_df = df_TFIDF.withColumn("features", MLVectorToString($"features")).drop("words").drop("rawFeatures")

    results_df.write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append

    sc.stop()
    spark.stop()

  }

  def FeatureExtractionModel_Word2Vec(model_path:String, ipt_table:String, opt_table:String) = {
    val SparkConf = new SparkConf().setAppName(s"FeatureExtractionModelApplication:Word2Vec").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val ipt_df = spark.read.jdbc(url, ipt_table, prop)

    // 加载模型
    val word2vec_model = PipelineModel.load(model_path)

    // Predict on the sentenceData dataset
    val df_Word2Vec = word2vec_model.transform(ipt_df)

    //将结果保存到数据框中
    // 列features的array类型转成string类型，因为mysql中没有array类型
    val MLVectorToString = udf((features:MLVector) => Vectors.fromML(features).toDense.toString())

    val results_df = df_Word2Vec.withColumn("features", MLVectorToString($"features")).drop("words")

    results_df.write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append
    sc.stop()
    spark.stop()

  }

  def FeatureExtractionModel_WordCount(model_path:String, ipt_table:String, opt_table:String) = {
    val SparkConf = new SparkConf().setAppName(s"FeatureExtractionModelApplication:WordCount").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val ipt_df = spark.read.jdbc(url, ipt_table, prop)

    // 加载模型
    val cv_model = PipelineModel.load(model_path)

    // Predict on the sentenceData dataset
    val df_CV = cv_model.transform(ipt_df)
    //将结果保存到数据框中

    // 列features的array类型转成string类型，因为mysql中没有array类型
    val MLVectorToString = udf((features:MLVector) => Vectors.fromML(features).toDense.toString())

    val results_df = df_CV.withColumn("features", MLVectorToString($"features")).drop("words")

    results_df.write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append

    sc.stop()
    spark.stop()

  }

}
