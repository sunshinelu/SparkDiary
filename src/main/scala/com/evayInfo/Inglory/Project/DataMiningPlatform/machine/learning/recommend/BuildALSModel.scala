package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties

import com.evayInfo.Inglory.Project.DataMiningPlatform.utils.{Constants, ConfigurationManager}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 18/10/24.
 *
 * 功能描述：
 * 使用ALS构建推荐算法模型
 *
 * 方法：
 * 显式，不含测试集:ALSModel
 * 显式，含测试集:ALSModel_test
 *
 *
 * 隐式，不含测试集:ALSModelImplicit
 * 隐式，含测试集:ALSModelImplicit_Test
 *
 * 注意：隐式模型和显式模型输入参数不同。
 *
 */
class BuildALSModel {

  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  // 链接mysql配置信息
  //  val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
  //    "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
  //  val user = "root"
  //  val password = "123456"
  val url: String = ConfigurationManager.getProperty(Constants.MYSQL_JDBC_URL)
  val user: String = ConfigurationManager.getProperty(Constants.MYSQL_JDBC_USER)
  val password: String = ConfigurationManager.getProperty(Constants.MYSQL_JDBC_PASSWORD)

  val prop = new Properties()
  prop.setProperty("user", user)
  prop.setProperty("password", password)


  /*
  输入参数为：
  train_table:输入训练集的表名，String类型
  user_col:用户名所在列名，String类型（该列的数据为String类型）
  item_col:商品品所在列名，String类型（该列的数据为String类型）
  rating_col:评分所在列名，String类型（该列的数据为Double类型）
  rank:建模所需参数rank，Int类型
  numIterations:建模所需参数numIterations，Int类型
  lambda:建模所需参数lambda,Double类型
  model_path:模型路径，String类型
   */
  def ALSModel(train_table:String,user_col:String,item_col:String,rating_col:String,rank:Int,numIterations:Int,lambda:Double,model_path:String)={

    val SparkConf = new SparkConf().setAppName(s"BuildALSModel:ALSModel").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user","item","rating")
    val train_df = spark.read.jdbc(url, train_table, prop).select(user_col,item_col,rating_col).toDF(col_name:_*)

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

    val indexer_model_path = model_path + "_indexer_model"
    indexer_model.write.overwrite().save(indexer_model_path)

    val indexer_df = indexer_model.transform(train_df)

    val train_pre_df = indexer_df.
      withColumn("user_id", $"user_id".cast("int")).
      withColumn("item_id", $"item_id".cast("int")).
      withColumn("rating", $"rating".cast("double"))

    val rating_rdd  = train_pre_df.select("user_id","item_id","rating").
      rdd.map{case Row(user_id:Int,item_id:Int, rating:Double) => Rating(user_id, item_id,rating)}

    // Build the recommendation model using ALS
//    val rank = 10
//    val numIterations = 10
//    val lambda = 0.01
    val als_model = ALS.train(rating_rdd, rank, numIterations, lambda)

    // Save and load model
    als_model.save(sc, model_path)



    sc.stop()
    spark.stop()
  }


  /*
 输入：
 train_table:输入训练集的表名，String类型
 user_col:用户名所在列名，String类型（该列的数据为String类型）
 item_col:商品品所在列名，String类型（该列的数据为String类型）
 rating_col:评分所在列名，String类型（该列的数据为Double类型）
 rank:建模所需参数rank，Int类型
 numIterations:建模所需参数numIterations，Int类型
 lambda:建模所需参数lambda,Double类型
 model_path:模型路径，String类型
 test_table:输入测试集的表名，String类型
 opt_table:输入测试集的表名，String类型
  */
  def ALSModel_test(train_table:String,user_col:String,item_col:String,rating_col:String,
                    rank:Int,numIterations:Int,lambda:Double,
                    model_path:String,test_table:String,opt_table:String)={

    val SparkConf = new SparkConf().setAppName(s"BuildALSModel:ALSModel_test").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user","item","rating")
    val train_df = spark.read.jdbc(url, train_table, prop).select(user_col,item_col,rating_col).toDF(col_name:_*)

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

    val indexer_model_path = model_path + "_indexer_model"
    indexer_model.write.overwrite().save(indexer_model_path)

    val indexer_df = indexer_model.transform(train_df)

    val train_pre_df = indexer_df.
      withColumn("user_id", $"user_id".cast("int")).
      withColumn("item_id", $"item_id".cast("int")).
      withColumn("rating", $"rating".cast("double"))

    val rating_rdd  = train_pre_df.select("user_id","item_id","rating").
      rdd.map{case Row(user_id:Int,item_id:Int, rating:Double) => Rating(user_id, item_id,rating)}

    // Build the recommendation model using ALS
    //    val rank = 10
    //    val numIterations = 10
    //    val lambda = 0.01
    val als_model = ALS.train(rating_rdd, rank, numIterations, lambda)

    // Save and load model
    als_model.save(sc, model_path)


    // 对测试结果进行预测
    val model_application = new ALSModelApplication()
    model_application.RatingPrediction(test_table,model_path,user_col,item_col,rating_col,opt_table)


    sc.stop()
    spark.stop()
  }






  /*
 输入：
 train_table:输入训练集的表名，String类型
 user_col:用户名所在列名，String类型（该列的数据为String类型）
 item_col:商品品所在列名，String类型（该列的数据为String类型）
 rating_col:评分所在列名，String类型（该列的数据为Double类型）
 rank:建模所需参数rank，Int类型
 numIterations:建模所需参数numIterations，Int类型
 lambda:建模所需参数lambda,Double类型
 alpha:建模所需参数alpha，Double类型
 model_path:模型路径，String类型
  */
  def ALSModelImplicit(train_table:String,user_col:String,item_col:String,rating_col:String,
                       rank:Int,numIterations:Int,lambda:Double,alpha:Double,
                       model_path:String)={

    val SparkConf = new SparkConf().setAppName(s"BuildALSModel:ALSModelImplicit").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user","item","rating")
    val train_df = spark.read.jdbc(url, train_table, prop).select(user_col,item_col,rating_col).toDF(col_name:_*)

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

    val indexer_model_path = model_path + "_indexer_model"
    indexer_model.write.overwrite().save(indexer_model_path)

    val indexer_df = indexer_model.transform(train_df)

    val train_pre_df = indexer_df.
      withColumn("user_id", $"user_id".cast("int")).
      withColumn("item_id", $"item_id".cast("int")).
      withColumn("rating", $"rating".cast("double"))

    val rating_rdd  = train_pre_df.select("user_id","item_id","rating").
      rdd.map{case Row(user_id:Int,item_id:Int, rating:Double) => Rating(user_id, item_id,rating)}

    // Build the recommendation model using ALS
    //    val rank = 10
    //    val numIterations = 10
    //    val lambda = 0.01
//    val alpha = 0.01
    val als_model = ALS.trainImplicit(rating_rdd, rank, numIterations, lambda, alpha)

    // Save and load model
    als_model.save(sc, model_path)



    sc.stop()
    spark.stop()
  }


  /*
 输入：
 train_table:输入训练集的表名，String类型
 user_col:用户名所在列名，String类型（该列的数据为String类型）
 item_col:商品品所在列名，String类型（该列的数据为String类型）
 rating_col:评分所在列名，String类型（该列的数据为Double类型）
 rank:建模所需参数rank，Int类型
 numIterations:建模所需参数numIterations，Int类型
 lambda:建模所需参数lambda,Double类型
 alpha:建模所需参数alpha，Double类型
 model_path:模型路径，String类型
 test_table:输入测试集的表名，String类型
 opt_table:输入测试集的表名，String类型
  */
  def ALSModelImplicit_Test(train_table:String,user_col:String,item_col:String,rating_col:String,
                            rank:Int,numIterations:Int,lambda:Double,alpha:Double,
                            model_path:String,test_table:String, opt_table:String)={

    val SparkConf = new SparkConf().setAppName(s"BuildALSModel:ALSModelImplicit_Test").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user","item","rating")
    val train_df = spark.read.jdbc(url, train_table, prop).select(user_col,item_col,rating_col).toDF(col_name:_*)

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

    val indexer_model_path = model_path + "_indexer_model"
    indexer_model.write.overwrite().save(indexer_model_path)

    val indexer_df = indexer_model.transform(train_df)

    val train_pre_df = indexer_df.
      withColumn("user_id", $"user_id".cast("int")).
      withColumn("item_id", $"item_id".cast("int")).
      withColumn("rating", $"rating".cast("double"))

    val rating_rdd  = train_pre_df.select("user_id","item_id","rating").
      rdd.map{case Row(user_id:Int,item_id:Int, rating:Double) => Rating(user_id, item_id,rating)}

    // Build the recommendation model using ALS
    //    val rank = 10
    //    val numIterations = 10
    //    val lambda = 0.01
    //    val alpha = 0.01
    val als_model = ALS.trainImplicit(rating_rdd, rank, numIterations, lambda, alpha)

    // Save and load model
    als_model.save(sc, model_path)


    // 对测试结果进行预测
    val model_application = new ALSModelApplication()
    model_application.RatingPrediction(test_table,model_path,user_col,item_col,rating_col,opt_table)

    sc.stop()
    spark.stop()
  }



}
