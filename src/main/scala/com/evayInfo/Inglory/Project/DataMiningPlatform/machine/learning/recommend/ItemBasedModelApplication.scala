package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties

import com.evayInfo.Inglory.Project.DataMiningPlatform.utils.{Constants, ConfigurationManager}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/10/26.
 */
class ItemBasedModelApplication {

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

  def RatingPrediction(test_table: String, model_name: String,
                       user_col: String, item_col: String, rating_col: String,
                       opt_table: String)={
    val SparkConf = new SparkConf().setAppName(s"ItemBasedModelApplication:RatingPrediction").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user", "item", "rating")
    val test_df = spark.read.jdbc(url, test_table, prop).select(user_col, item_col, rating_col).toDF(col_name: _*)

    // 读取物品相似性结果
    val item_simi_df = spark.read.jdbc(url, model_name, prop)

    // user1, user2, userSimi, userString, itemString, rating
    val itemR_1 = item_simi_df.join(test_df, item_simi_df("item_1") === test_df("item"), "left").
      withColumn("recomValue", col("simi") * col("rating")).
      groupBy("item_2", "user").agg(sum($"recomValue")).drop("recomValue").
      withColumnRenamed("sum(recomValue)", "recomValue")

    val temp_df = test_df.withColumnRenamed("item", "item_2").withColumn("whether", lit(1))
    val itemR_2 = itemR_1.join(temp_df, Seq("item_2", "user"), "fullouter").filter(col("whether").isNotNull)


    /*
     根据 输入数据的rating的最大值最小值对 预测的结果进行标准化
      */
    val (rating_Min, rating_Max) = test_df.agg(min($"rating"), max($"rating")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }
    val scaled_Range = lit(rating_Max-rating_Min) // Range of the scaled variable
    val scaled_Min = lit(rating_Min) // Min value of the scaled variable

    val (recom_Min, recom_Max) = itemR_2.agg(min($"recomValue"), max($"recomValue")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }

    val recom_Normalized = ($"recomValue" - recom_Min) / (recom_Max - recom_Min) // v normalized to (0, 1) range
    val scaled = scaled_Range * recom_Normalized + scaled_Min
    val predict_df = itemR_2.withColumn("vScaled", scaled).
      drop("recomValue").withColumnRenamed("vScaled", "prediction").
      withColumnRenamed("item_2","item").drop("whether").
      na.fill(Map("prediction" -> 0.0)).
      withColumn("prediction", bround($"prediction", 3)) // 保留3位有效数字


    // 将预测结果保存到mysql数据库中
    predict_df.write.mode(SaveMode.Overwrite).jdbc(url,opt_table,prop)

    sc.stop()
    spark.stop()

  }

  def TopNProductsForUsers(test_table: String, model_name: String,
                           user_col: String, item_col: String, rating_col: String,
                           topN:Int,opt_table: String)={
    val SparkConf = new SparkConf().setAppName(s"ItemBasedModelApplication:TopNProductsForUsers").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user", "item", "rating")
    val test_df = spark.read.jdbc(url, test_table, prop).select(user_col, item_col, rating_col).toDF(col_name: _*)

    // 读取物品相似性结果
    val item_simi_df = spark.read.jdbc(url, model_name, prop)

    // user1, user2, userSimi, userString, itemString, rating
    val itemR_1 = item_simi_df.join(test_df, item_simi_df("item_1") === test_df("item"), "left").
      withColumn("recomValue", col("simi") * col("rating")).
      groupBy("item_2", "user").agg(sum($"recomValue")).drop("recomValue").
      withColumnRenamed("sum(recomValue)", "recomValue")

    /*
     根据 输入数据的rating的最大值最小值对 预测的结果进行标准化
      */
    val (rating_Min, rating_Max) = test_df.agg(min($"rating"), max($"rating")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }
    val scaled_Range = lit(rating_Max-rating_Min) // Range of the scaled variable
    val scaled_Min = lit(rating_Min) // Min value of the scaled variable

    val (recom_Min, recom_Max) = itemR_1.agg(min($"recomValue"), max($"recomValue")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }

    val recom_Normalized = ($"recomValue" - recom_Min) / (recom_Max - recom_Min) // v normalized to (0, 1) range
    val scaled = scaled_Range * recom_Normalized + scaled_Min

    val itemR_1_Norm = itemR_1.withColumn("vScaled", scaled).
      drop("recomValue").withColumnRenamed("vScaled", "prediction")

    val temp_df = test_df.select("user", "item").withColumnRenamed("item", "item_2").withColumn("whether", lit(1))
    val itemR_2 = itemR_1_Norm.join(temp_df, Seq("item_2", "user"), "fullouter").filter(col("whether").isNull)


    val predict_df = itemR_2.withColumnRenamed("item_2","item")

    val w = Window.partitionBy("user").orderBy(col("prediction").desc)
    val result_df = predict_df.withColumn("rn", row_number.over(w)).where(col("rn") <= topN).
      drop("rn").drop("whether").na.fill(Map("prediction" -> 0.0)).
      withColumn("prediction", bround($"prediction", 3)) // 保留3位有效数字


    // 将结果保存到mysql数据库中
    result_df.write.mode(SaveMode.Overwrite).jdbc(url,opt_table,prop)

    sc.stop()
    spark.stop()

  }

  def TopNUsersForProducts(test_table: String, model_name: String,
                           user_col: String, item_col: String, rating_col: String,
                           topN:Int,opt_table: String)={
    val SparkConf = new SparkConf().setAppName(s"ItemBasedModelApplication:TopNUsersForProducts").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val col_name = Seq("user", "item", "rating")
    val test_df = spark.read.jdbc(url, test_table, prop).select(user_col, item_col, rating_col).toDF(col_name: _*)

    // 读取物品相似性结果
    val item_simi_df = spark.read.jdbc(url, model_name, prop)

    // user1, user2, userSimi, userString, itemString, rating
    val itemR_1 = item_simi_df.join(test_df, item_simi_df("item_1") === test_df("item"), "left").
      withColumn("recomValue", col("simi") * col("rating")).
      groupBy("item_2", "user").agg(sum($"recomValue")).drop("recomValue").
      withColumnRenamed("sum(recomValue)", "recomValue")

    /*
     根据 输入数据的rating的最大值最小值对 预测的结果进行标准化
      */
    val (rating_Min, rating_Max) = test_df.agg(min($"rating"), max($"rating")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }
    val scaled_Range = lit(rating_Max-rating_Min) // Range of the scaled variable
    val scaled_Min = lit(rating_Min) // Min value of the scaled variable

    val (recom_Min, recom_Max) = itemR_1.agg(min($"recomValue"), max($"recomValue")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }

    val recom_Normalized = ($"recomValue" - recom_Min) / (recom_Max - recom_Min) // v normalized to (0, 1) range
    val scaled = scaled_Range * recom_Normalized + scaled_Min

    val itemR_1_Norm = itemR_1.withColumn("vScaled", scaled).
      drop("recomValue").withColumnRenamed("vScaled", "prediction")

    val temp_df = test_df.select("user", "item").withColumnRenamed("item", "item_2").withColumn("whether", lit(1))
    val itemR_2 = itemR_1_Norm.join(temp_df, Seq("item_2", "user"), "fullouter").filter(col("whether").isNull)


    val predict_df = itemR_2.withColumnRenamed("item_2","item")

    val w = Window.partitionBy("item").orderBy(col("prediction").desc)
    val result_df = predict_df.withColumn("rn", row_number.over(w)).where(col("rn") <= topN).
      drop("rn").drop("whether").na.fill(Map("prediction" -> 0.0)).
      withColumn("prediction", bround($"prediction", 3)) // 保留3位有效数字


    // 将结果保存到mysql数据库中
    result_df.write.mode(SaveMode.Overwrite).jdbc(url,opt_table,prop)

    sc.stop()
    spark.stop()
  }


}
