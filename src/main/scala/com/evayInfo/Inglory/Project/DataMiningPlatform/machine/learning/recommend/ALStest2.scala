package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/10/24.
 * 使用MLlib构建推荐模型
 */
object ALStest2 {

  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class RatingSchema(user_id: Int, item_id: Int, rating: Double)


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

    val item_indexer = new StringIndexer()
      .setInputCol("item")
      .setOutputCol("item_id")


    // Create our pipeline
    val indexer_pipeline = new Pipeline().setStages(Array(user_indexer, item_indexer))
    // Train the model
    val indexer_model = indexer_pipeline.fit(ipt_df)

    val indexer_model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/indexer_model"
    indexer_model.write.overwrite().save(indexer_model_path)
    val indexer_model_load = PipelineModel.load(indexer_model_path)

    val indexer_df = indexer_model.transform(ipt_df)

    val train_df = indexer_df.
      withColumn("user_id", $"user_id".cast("int")).
      withColumn("item_id", $"item_id".cast("int")).
      withColumn("rating", $"rating".cast("double"))

    val rating_rdd  = train_df.select("user_id","item_id","rating").
      rdd.map{case Row(user_id:Int,item_id:Int, rating:Double) => Rating(user_id, item_id,rating)}

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val als_model = ALS.train(rating_rdd, rank, numIterations, 0.01)
    // Save and load model
    val als_model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/als_model_rdd"
//    als_model.save(sc, als_model_path)
    val als_model_load = MatrixFactorizationModel.load(sc, als_model_path)


    // Evaluate the model on rating data
    val usersProducts = rating_rdd.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      als_model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = rating_rdd.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

    val col_name = Seq("user_id", "item_id","prediction")
    val predictions_df =
      als_model.predict(usersProducts).map { case Rating(user, product, rate) =>
        (user, product, rate)
      }.toDF(col_name: _*).join(train_df,Seq("user_id","item_id"),"left").
        withColumn("prediction", bround($"prediction", 3)) // 保留3位有效数字

    predictions_df.show(truncate = false)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions_df)
    println(s"Root-mean-square error = $rmse")


    val item_number = 10
    val topItems = als_model.recommendProductsForUsers(item_number).flatMap(x => {
      val y = x._2
      for (w <- y) yield (w.user, w.product, w.rating)
    }).map{x => RatingSchema(x._1, x._2, x._3)}.toDF(col_name:_*).
      withColumn("prediction", bround($"prediction", 3)) // 保留3位有效数字

    val userLab = train_df.select("user", "user_id").dropDuplicates
    val itemLab = train_df.select("item", "item_id").dropDuplicates

    val item_join_df = topItems.join(userLab, Seq("user_id"), "left")
    val topItems_result_df = item_join_df.join(itemLab, Seq("item_id"), "left")

    println("recommender top item for users: ")
    topItems_result_df.show(truncate = false)

    val user_number = 20
    val topUsers = als_model.recommendUsersForProducts(user_number).flatMap(x => {
      val y = x._2
      for (w <- y) yield (w.user, w.product, w.rating)
    }).map{x => RatingSchema(x._1, x._2, x._3)}.toDF(col_name:_*).
      withColumn("prediction", bround($"prediction", 3)) // 保留3位有效数字

    val user_join_df = topUsers.join(userLab, Seq("user_id"), "left")
    val topUsers_result_df = user_join_df.join(itemLab, Seq("item_id"), "left")
    println("recommender top users for item: ")
    topUsers_result_df.show(truncate = false)




    sc.stop()
    spark.stop()

  }
}
