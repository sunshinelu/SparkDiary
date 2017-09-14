package com.evayInfo.Inglory.TestCode

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/9/14.
 */
object wDFtest {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(s"combinedModel").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val w_als = 0.25
    val w_content = 0.25
    val w_item = 0.25
    val w_user = 0.25
    val w_df = sc.parallelize(Seq(("als", w_als), ("content", w_content), ("item", w_item), ("user", w_user))).
      toDF("", "")
    w_df.show()

    sc.stop()
    spark.stop()

  }
}
