package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 18/10/26.
 * 构建基于物品的协同过滤模型
 */

case class ItemSimi(item_id_1: Long, item_id2: Long, similar: Double)
class BuildItemBasedModel {

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

  def ItemBased(train_table:String,user_col:String,item_col:String,rating_col:String,
                     simi_threshold:Double,model_name:String)={
    val SparkConf = new SparkConf().setAppName(s"BuildItemBasedModel:ItemBased").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user","item","rating")
    val train_df = spark.read.jdbc(url, train_table, prop).select(user_col,item_col,rating_col).toDF(col_name:_*)

    // string to index
    val user_indexer = new StringIndexer()
      .setInputCol("user")
      .setOutputCol("user_id")

    val item_indexer = new StringIndexer()
      .setInputCol("item")
      .setOutputCol("item_id")

    // Create our pipeline
    val indexer_pipeline = new Pipeline().setStages(Array(user_indexer, item_indexer))
    // Train the model
    val indexer_model = indexer_pipeline.fit(train_df)

    val indexer_df = indexer_model.transform(train_df)

    val train_pre_df = indexer_df.
      withColumn("user_id", $"user_id".cast("long")).
      withColumn("item_id", $"item_id".cast("long")).
      withColumn("rating", $"rating".cast("double"))

    val rating_rdd  = train_pre_df.select("user_id","item_id","rating").
      rdd.map{case Row(user_id:Long,item_id:Long, rating:Double) => MatrixEntry(user_id, item_id,rating)}
    //calculate similarities
    val ratings = new CoordinateMatrix(rating_rdd)
    //    val simi_threshold = 0.1
    val item_simi = ratings.toRowMatrix.columnSimilarities(simi_threshold)

    val simi_col_name = Seq("item_id_1", "item_id_2","simi")
    val item_id_simi_df = item_simi.entries.map(f => ItemSimi(f.i, f.j, f.value)).
      union(item_simi.entries.map(f => ItemSimi(f.j, f.i, f.value))).toDF(simi_col_name:_*)

    val item_1_lab = train_pre_df.select("item_id","item").toDF("item_id_1","item_1")
    val item_2_lab = train_pre_df.select("item_id","item").toDF("item_id_2","item_2")
    val item_simi_df = item_id_simi_df.join(item_1_lab, Seq("item_id_1"), "left").
      join(item_2_lab, Seq("item_id_2"), "left").na.drop.select("item_1", "item_2", "simi")

    // 保存 user_simi_df 到mysql
    item_simi_df.write.mode("overwrite").jdbc(url, model_name, prop) //overwrite ; append


    sc.stop()
    spark.stop()

  }

  def ItemBased_test(train_table:String,user_col:String,item_col:String,rating_col:String,
                     simi_threshold:Double,
                     model_name:String,test_table:String,opt_table:String)={
    val SparkConf = new SparkConf().setAppName(s"BuildItemBasedModel:ItemBased_test").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user","item","rating")
    val train_df = spark.read.jdbc(url, train_table, prop).select(user_col,item_col,rating_col).toDF(col_name:_*)

    // string to index
    val user_indexer = new StringIndexer()
      .setInputCol("user")
      .setOutputCol("user_id")

    val item_indexer = new StringIndexer()
      .setInputCol("item")
      .setOutputCol("item_id")

    // Create our pipeline
    val indexer_pipeline = new Pipeline().setStages(Array(user_indexer, item_indexer))
    // Train the model
    val indexer_model = indexer_pipeline.fit(train_df)

    val indexer_df = indexer_model.transform(train_df)

    val train_pre_df = indexer_df.
      withColumn("user_id", $"user_id".cast("long")).
      withColumn("item_id", $"item_id".cast("long")).
      withColumn("rating", $"rating".cast("double"))

    val rating_rdd  = train_pre_df.select("user_id","item_id","rating").
      rdd.map{case Row(user_id:Long,item_id:Long, rating:Double) => MatrixEntry(user_id, item_id,rating)}
    //calculate similarities
    val ratings = new CoordinateMatrix(rating_rdd)
    //    val simi_threshold = 0.1
    val item_simi = ratings.toRowMatrix.columnSimilarities(simi_threshold)

    val simi_col_name = Seq("item_id_1", "item_id_2","simi")
    val item_id_simi_df = item_simi.entries.map(f => ItemSimi(f.i, f.j, f.value)).
      union(item_simi.entries.map(f => ItemSimi(f.j, f.i, f.value))).toDF(simi_col_name:_*)

    val item_1_lab = train_pre_df.select("item_id","item").toDF("item_id_1","item_1")
    val item_2_lab = train_pre_df.select("item_id","item").toDF("item_id_2","item_2")
    val item_simi_df = item_id_simi_df.join(item_1_lab, Seq("item_id_1"), "left").
      join(item_2_lab, Seq("item_id_2"), "left").na.drop.select("item_1", "item_2", "simi")

    // 保存 user_simi_df 到mysql
    item_simi_df.write.mode("overwrite").jdbc(url, model_name, prop) //overwrite ; append

    // 对输入的测试集进行预测
    val model_application = new ItemBasedModelApplication()
    model_application.RatingPrediction(test_table,model_name,user_col,item_col,rating_col,opt_table)

    sc.stop()
    spark.stop()

  }
}
