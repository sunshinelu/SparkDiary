package com.evayInfo.Inglory.Project.Recommend.AppRecom

import java.util.Properties

import com.evayInfo.Inglory.Project.Recommend.RecomUtil
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/8/25.
 */
object UsersimiSub {

  case class UserSimi(userId1: Long, userId2: Long, similar: Double)

  def main(args: Array[String]) {
    RecomUtil.SetLogger

    val conf = new SparkConf().setAppName(s"UsersimiSub").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val url1 = "jdbc:mysql://localhost:3306/ylzx?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver")
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "YLZX_NRGL_MYSUB_WEBSITE_COL", prop1)
    //    ds1.printSchema()
    /*
    root
 |-- OPERATOR_ID: string (nullable = false)
 |-- WEBSITE_ID: string (nullable = false)
 |-- COLUMN_ID: string (nullable = false)
 |-- COLUMN_NAME: string (nullable = true)
 |-- COLUMN_URL: string (nullable = true)
     */
    //    println("ds1 is: " + ds1.count())

    val ds2 = ds1.select("OPERATOR_ID", "COLUMN_ID").withColumn("value", lit(1)).na.drop()
    ds2.printSchema()
    println("ds2 is: " + ds2.count())
    //string to number
    val userID = new StringIndexer().setInputCol("OPERATOR_ID").setOutputCol("userID").fit(ds2)
    val ds3 = userID.transform(ds2)
    val urlID = new StringIndexer().setInputCol("COLUMN_ID").setOutputCol("urlID").fit(ds3)
    val ds4 = urlID.transform(ds3)
    ds4.printSchema()

    val ds5 = ds4.withColumn("userID", ds4("userID").cast("long")).
      withColumn("urlID", ds4("urlID").cast("long")).
      withColumn("value", ds4("value").cast("double"))
    ds5.printSchema()

    val userLab = ds5.select("userID", "OPERATOR_ID").na.drop()
    val itemLab = ds5.select("urlID", "COLUMN_ID").na.drop()

    //RDD to RowRDD
    val rdd1 = ds5.select("userID", "urlID", "value").rdd.
      map { case Row(user: Long, item: Long, value: Double) => MatrixEntry(user, item, value) }

    //calculate similarities
    val ratings = new CoordinateMatrix(rdd1).transpose()
    val userSimi = ratings.toRowMatrix.columnSimilarities(0.1)
    // user-user similarity
    val userSimiRdd = userSimi.entries.map(f => UserSimi(f.i, f.j, f.value))

    // user1, user1, similar
    val userSimiDF = userSimiRdd.map { f => (f.userId1, f.userId2, f.similar) }.
      union(userSimiRdd.map { f => (f.userId2, f.userId1, f.similar) }).toDF("userID", "urlID", "value")

    val ds6_0 = userSimiDF.withColumn("userID", col("userID").cast("double")).
      withColumn("urlID", col("urlID").cast("double"))

    val indexer1 = new IndexToString()
      .setInputCol("userID")
      .setOutputCol("userString")

    val ds6 = indexer1.transform(ds6_0)


    val indexer2 = new IndexToString()
      .setInputCol("urlID")
      .setOutputCol("urlString")

    val ds7 = indexer2.transform(ds6)
    ds7.printSchema()


    val ds8 = ds7.join(userLab, Seq("userID"), "left").join(itemLab, Seq("urlID"), "left")
    ds8.printSchema()
    ds8.take(5).foreach(println)


    sc.stop()
    spark.stop()
  }

}
