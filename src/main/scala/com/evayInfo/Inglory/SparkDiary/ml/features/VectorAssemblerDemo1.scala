package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by sunlu on 17/6/23.
  * 将dataframe中的某几列拼接成features
  *
  * 参考链接：https://stackoverflow.com/questions/31028806/how-to-create-correct-data-frame-for-classification-in-spark-ml
  * https://spark.apache.org/docs/latest/ml-features.html#vectorassembler
  */
object VectorAssemblerDemo1 {
  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.WARN)

    //bulid environment
    val spark = SparkSession.builder.appName("VectorAssemblerDemo1").master("local[*]").getOrCreate()
    val sc = spark.sparkContext


    val dataset = spark.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    dataset.show()

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)
    output.printSchema()
    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)

  }
}
