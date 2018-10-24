package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

/**
 * Created by sunlu on 18/10/22.
 * 使用ML构建推荐模型
 */
object ALStest1 {

  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    // 链接mysql配置信息
    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)

    SetLogger
    val SparkConf = new SparkConf().setAppName(s"ALStest1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ipt_table = "recommenderSys_Demo_Data_sample"
    val ipt_df = spark.read.jdbc(url, ipt_table,prop)

    val user_indexer = new StringIndexer()
      .setInputCol("user")
      .setOutputCol("user_id")
//    val user_indexed_model = user_indexer.fit(ipt_df)
//    user_indexed_model
//    val user_indexed_df = user_indexer.fit(ipt_df).transform(ipt_df)

    val item_indexer = new StringIndexer()
      .setInputCol("item")
      .setOutputCol("item_id")
//    val item_indexed_df = item_indexer.fit(user_indexed_df).transform(user_indexed_df)

//    item_indexed_df.printSchema()

    // Create our pipeline
    val indexer_pipeline = new Pipeline().setStages(Array(user_indexer, item_indexer))
    // Train the model
    val indexer_model = indexer_pipeline.fit(ipt_df)

    val indexer_model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/indexer_model"
    indexer_model.write.overwrite().save(indexer_model_path)
    val indexer_model_load = PipelineModel.load(indexer_model_path)

    val indexer_df = indexer_model.transform(ipt_df)

    val train_df = indexer_df.
      withColumn("user_id", $"user_id".cast("long")).
      withColumn("item_id", $"item_id".cast("long")).
      withColumn("rating", $"rating".cast("double"))

    train_df.show(truncate = false)
    train_df.printSchema()


    val max_iter = 5
    val reg_param = 0.01
    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(max_iter)
      .setRegParam(reg_param)
      .setUserCol("user_id")
      .setItemCol("item_id")
      .setRatingCol("rating")
    val als_model = als.fit(train_df)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = als_model.transform(train_df)
    predictions.show(truncate = false)
    predictions.filter(! $"".contains(""))
    predictions.where(! $"".contains(""))


    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    // Root-mean-square error = 0.0014726680465764915


    sc.stop()
    spark.stop()
  }

}
