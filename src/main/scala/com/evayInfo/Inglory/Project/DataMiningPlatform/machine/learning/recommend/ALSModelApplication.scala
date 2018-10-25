package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 18/10/24.
 */


case class RatingSchema(user_id: Int, item_id: Int, rating: Double)

class ALSModelApplication {

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

  def RatingPrediction(test_table: String, model_path: String,
                       user_col: String, item_col: String, rating_col: String,
                       opt_table: String) = {
    val indexer_model_path = model_path + "_indexer_model"


    val SparkConf = new SparkConf().setAppName(s"ALSModelApplication:RatingPrediction").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user", "item", "rating")
    val test_df = spark.read.jdbc(url, test_table, prop).select(user_col, item_col, rating_col).toDF(col_name: _*)

    val indexer_model_reload = PipelineModel.load(indexer_model_path)

    val test_indexer_df = indexer_model_reload.transform(test_df)

    val test_prep_df = test_indexer_df.
      withColumn("user_id", $"user_id".cast("int")).
      withColumn("item_id", $"item_id".cast("int")).
      withColumn("rating", $"rating".cast("double"))

    val test_rating_rdd = test_prep_df.select("user_id", "item_id", "rating").
      rdd.map { case Row(user_id: Int, item_id: Int, rating: Double) => Rating(user_id, item_id, rating) }

    val als_model_reload = MatrixFactorizationModel.load(sc, model_path)
    // Evaluate the model on rating data
    val usersProducts = test_rating_rdd.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val pred_col_name = Seq("user_id", "item_id", "prediction")
    val predictions_df =
      als_model_reload.predict(usersProducts).map { case Rating(user, product, rate) =>
        (user, product, rate)
      }.toDF(pred_col_name: _*).join(test_prep_df, Seq("user_id", "item_id"), "left").
        withColumn("prediction", bround($"prediction", 3)) // 保留3位有效数字

    //将结果保存到数据框中
    predictions_df.drop("user_id").drop("item_id").write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append

    sc.stop()
    spark.stop()
  }

  def TopNProductsForUsers(test_table: String, model_path: String,
                           user_col: String, item_col: String, rating_col: String,
                           topN: Int, opt_table: String) = {
    val indexer_model_path = model_path + "_indexer_model"


    val SparkConf = new SparkConf().setAppName(s"ALSModelApplication:TopNProductsForUsers").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user", "item", "rating")
    val test_df = spark.read.jdbc(url, test_table, prop).select(user_col, item_col, rating_col).toDF(col_name: _*)

    val indexer_model_reload = PipelineModel.load(indexer_model_path)

    val test_indexer_df = indexer_model_reload.transform(test_df)

    val test_prep_df = test_indexer_df.
      withColumn("user_id", $"user_id".cast("int")).
      withColumn("item_id", $"item_id".cast("int")).
      withColumn("rating", $"rating".cast("double"))

    val test_rating_rdd = test_prep_df.select("user_id", "item_id", "rating").
      rdd.map { case Row(user_id: Int, item_id: Int, rating: Double) => Rating(user_id, item_id, rating) }

    val als_model_reload = MatrixFactorizationModel.load(sc, model_path)
    // recommender topN Products For Users

    val pred_col_name = Seq("user_id", "item_id", "prediction")
      val topItems = als_model_reload.recommendProductsForUsers(topN).flatMap(x => {
        val y = x._2
        for (w <- y) yield (w.user, w.product, w.rating)
      }).map { x => RatingSchema(x._1, x._2, x._3) }.toDF(pred_col_name: _*).
        withColumn("prediction", bround($"prediction", 3)) // 保留3位有效数字

      val userLab = test_prep_df.select("user", "user_id").dropDuplicates
      val itemLab = test_prep_df.select("item", "item_id").dropDuplicates

      val item_join_df = topItems.join(userLab, Seq("user_id"), "left")
      val topItems_result_df = item_join_df.join(itemLab, Seq("item_id"), "left")

      //将结果保存到数据框中
      topItems_result_df.drop("user_id").drop("item_id").write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append

      sc.stop()
      spark.stop()

  }


  def TopNUsersForProducts(test_table: String, model_path: String,
                           user_col: String, item_col: String, rating_col: String,
                           topN: Int, opt_table: String) = {
    val indexer_model_path = model_path + "_indexer_model"


    val SparkConf = new SparkConf().setAppName(s"ALSModelApplication:TopNUsersForProducts").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user", "item", "rating")
    val test_df = spark.read.jdbc(url, test_table, prop).select(user_col, item_col, rating_col).toDF(col_name: _*)

    val indexer_model_reload = PipelineModel.load(indexer_model_path)

    val test_indexer_df = indexer_model_reload.transform(test_df)

    val test_prep_df = test_indexer_df.
      withColumn("user_id", $"user_id".cast("int")).
      withColumn("item_id", $"item_id".cast("int")).
      withColumn("rating", $"rating".cast("double"))

    val test_rating_rdd = test_prep_df.select("user_id", "item_id", "rating").
      rdd.map { case Row(user_id: Int, item_id: Int, rating: Double) => Rating(user_id, item_id, rating) }

    val als_model_reload = MatrixFactorizationModel.load(sc, model_path)
    // recommender topN Products For Users

    val pred_col_name = Seq("user_id", "item_id", "prediction")
    val topUsers = als_model_reload.recommendUsersForProducts(topN).flatMap(x => {
      val y = x._2
      for (w <- y) yield (w.user, w.product, w.rating)
    }).map{x => RatingSchema(x._1, x._2, x._3)}.toDF(pred_col_name:_*).
      withColumn("prediction", bround($"prediction", 3)) // 保留3位有效数字

    val userLab = test_prep_df.select("user", "user_id").dropDuplicates
    val itemLab = test_prep_df.select("item", "item_id").dropDuplicates


    val user_join_df = topUsers.join(userLab, Seq("user_id"), "left")
    val topUsers_result_df = user_join_df.join(itemLab, Seq("item_id"), "left")

    //将结果保存到数据框中
    topUsers_result_df.drop("user_id").drop("item_id").write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append

    sc.stop()
    spark.stop()

  }


}
