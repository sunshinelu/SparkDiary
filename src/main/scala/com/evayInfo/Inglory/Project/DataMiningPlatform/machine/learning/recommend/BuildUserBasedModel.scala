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
 * 功能描述：构建基于用户的协同过滤模型
 */

case class UserSimi(user_id_1: Long, user_id_2: Long, similar: Double)
case class UserRecomm(user_id: Long, item_id: Long, rating: Double)

class BuildUserBasedModel {

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


  def UserBasedModel(train_table:String,user_col:String,item_col:String,rating_col:String,simi_threshold:Double,model_path:String)={
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
      withColumn("user_id", $"user_id".cast("long")).
      withColumn("item_id", $"item_id".cast("long")).
      withColumn("rating", $"rating".cast("double"))

    val rating_rdd  = train_pre_df.select("user_id","item_id","rating").
      rdd.map{case Row(user_id:Long,item_id:Long, rating:Double) => MatrixEntry(user_id, item_id,rating)}

    //calculate similarities
    val ratings = new CoordinateMatrix(rating_rdd).transpose()
//    val simi_threshold = 0.1
    val userSimi = ratings.toRowMatrix.columnSimilarities(simi_threshold)
    // user-user similarity
    val userSimiRdd = userSimi.entries.map(f => UserSimi(f.i, f.j, f.value))

    val simi_col_name = Seq("user_id_1", "user_id_2","simi")
    val userSimi_df = userSimiRdd.map { f => (f.user_id_1, f.user_id_2, f.similar) }.
      union(userSimiRdd.map { f => (f.user_id_2, f.user_id_1, f.similar) }).toDF(simi_col_name:_*)

    // user1, user1, similar
    val rdd_app_R1 = userSimiRdd.map { f => (f.user_id_1, f.user_id_2, f.similar) }.
      union(userSimiRdd.map { f => (f.user_id_2, f.user_id_1, f.similar) })

    // user item value
    val user_prefer = rating_rdd.map { f => (f.i, f.j, f.value) }

    // user1, [(user1, similar),(item, value)]
    val rdd_app_R2 = rdd_app_R1.map { f => (f._1, (f._2, f._3)) }.join(user_prefer.map(f => (f._1, (f._2, f._3))))
    //    rdd_app_R2.collect().foreach(println)
    val rdd_app_R3 = rdd_app_R2.map { f => ((f._2._1._1, f._2._2._1), f._2._2._2 * f._2._1._2) }
    //    rdd_app_R3.collect().foreach(println)
    val rdd_app_R4 = rdd_app_R3.reduceByKey((x, y) => x + y)

    val rdd_app_R5 = rdd_app_R4.leftOuterJoin(user_prefer.map(f => ((f._1, f._2), 1))).
      filter(f => f._2._2.isEmpty).map(f => (f._1._1, (f._1._2, f._2._1)))
    val rdd_app_R6 = rdd_app_R5.groupByKey()

    val r_number = 30
    val rdd_app_R7 = rdd_app_R6.map { f =>
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > r_number) i2_2.remove(0, (i2_2.length - r_number))
      (f._1, i2_2.toIterable)
    }

    val rdd_app_R8 = rdd_app_R7.flatMap(f => {
      val id2 = f._2
      for (w <- id2) yield (f._1, w._1, w._2)
    }
    )

    //RDD to RowRDD
    val itemRecomm = rdd_app_R8.map { f => UserRecomm(f._1, f._2, f._3) }
    //RowRDD to DF
    val itemRecomDF = spark.createDataset(itemRecomm)

    val userLab = train_pre_df.select(user_col, "user_id").dropDuplicates
    val itemLab = train_pre_df.select(item_col, "item_id").dropDuplicates
    val joinDF1 = itemRecomDF.join(userLab, Seq("user_id"), "left")
    val joinDF2 = joinDF1.join(itemLab, Seq("item_id"), "left")



    sc.stop()
    spark.stop()
  }






}
