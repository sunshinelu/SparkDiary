package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 * Created by sunlu on 18/10/31.
 */
object UserBasedTest1 {

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
    val SparkConf = new SparkConf().setAppName(s"UserBasedTest1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    val col_name = Seq("user","item","rating")
    val table_name = "recommender_test"
    val df1 = spark.read.csv("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/user-based-recomme.csv").
      toDF(col_name:_*).withColumn("rating", $"rating".cast("double"))
    df1.write.mode(SaveMode.Overwrite).jdbc(url, table_name, prop) //overwrite ; append
*/

    val col_name = Seq("user","item","rating")
    val ipt_table = "recommender_test"
    val df1 = spark.read.jdbc(url,ipt_table,prop).toDF(col_name:_*)
    val train_df = df1.withColumn("user_id", $"user".cast("long")).
      withColumn("item_id", $"item".cast("long")).
      withColumn("rating", $"rating".cast("double"))

    val rating_rdd  = train_df.select("user_id","item_id","rating").
      rdd.map{case Row(user_id:Long,item_id:Long, rating:Double) => MatrixEntry(user_id, item_id,rating)}
    //calculate similarities
    val ratings = new CoordinateMatrix(rating_rdd).transpose()
    //    val simi_threshold = 0.1
    val userSimi = ratings.toRowMatrix.columnSimilarities()
    // user-user similarity
    val userSimiRdd = userSimi.entries.map(f => UserSimi(f.i, f.j, f.value))

    val simi_col_name = Seq("user_1", "user_2","simi")
    val user_id_simi_df = userSimiRdd.map { f => (f.user_id_1, f.user_id_2, f.similar) }.
      union(userSimiRdd.map { f => (f.user_id_2, f.user_id_1, f.similar) }).toDF(simi_col_name:_*)

    user_id_simi_df.show(truncate = false)

    // user1, user2, userSimi, userString, itemString, rating
    val userR_1 = user_id_simi_df.join(train_df, user_id_simi_df("user_1") === train_df("user"), "left").
      withColumn("recomValue", col("simi") * col("rating")).
      groupBy("user_2", "item").agg(avg($"recomValue")).drop("recomValue").
      withColumnRenamed("avg(recomValue)", "recomValue")
    userR_1.show(truncate = false)
    val temp_df = train_df.select("user", "item","rating").withColumnRenamed("user", "user_2").withColumn("whether", lit(1))
    val userR_2 = userR_1.join(temp_df, Seq("user_2", "item"), "fullouter").filter(col("whether").isNull)
//    'inner', 'outer', 'full', 'fullouter', 'leftouter', 'left', 'rightouter', 'right', 'leftsemi', 'leftanti', 'cross'.

    println("=============")
    userR_2.show(truncate = false)
    println("userR_2 count is:" + userR_2.count())



    val (rating_Min, rating_Max) = train_df.agg(min($"rating"), max($"rating")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }

    val scaled_Range = lit(rating_Max-rating_Min) // Range of the scaled variable
    val scaled_Min = lit(rating_Min) // Min value of the scaled variable

    val (recom_Min, recom_Max) = userR_2.agg(min($"recomValue"), max($"recomValue")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }
    println("scaled_Min is: "+ scaled_Min)
    println("scaled_Range is: "+ scaled_Range)
    println("recom_Min is: "+ recom_Min)
    println("recom_Max is: "+ recom_Max)

    val recom_Normalized = ($"recomValue" - recom_Min) / (recom_Max - recom_Min) // v normalized to (0, 1) range
    val scaled = scaled_Range * recom_Normalized + scaled_Min
    val predict_df = userR_2.withColumn("vScaled", scaled)//.
//      drop("recomValue").withColumnRenamed("vScaled", "prediction").withColumnRenamed("user_2","user").drop("whether")


    println("---------------------")
    predict_df.show(truncate = false)


    sc.stop()
    spark.stop()

  }
}
