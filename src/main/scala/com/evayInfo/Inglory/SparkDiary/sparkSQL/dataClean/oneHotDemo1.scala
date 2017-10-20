package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by sunlu on 17/10/19.
 * 参考链接：
 * Spark2 oneHot编码--标准化--主成分--聚类
 * http://www.cnblogs.com/wwxbi/p/6028175.html#3689490
 */
object oneHotDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"oneHotDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(
      (0, "cat26", 30.9), (0, "cat13", 22.1), (0, "cat95", 19.6), (0, "cat105", 1.3),
      (1, "cat67", 28.5), (1, "cat4", 26.8), (1, "cat13", 12.6), (1, "cat23", 5.3),
      (2, "cat56", 39.6), (2, "cat40", 29.7), (2, "cat187", 27.9), (2, "cat68", 9.8),
      (3, "cat8", 35.6))).toDF("Hour", "Category", "TotalValue")

    //字符转换成数字索引
    val indexer = new StringIndexer().
      setInputCol("Category").
      setOutputCol("CategoryIndex").
      fit(df)
    val indexed = indexer.transform(df)
    indexed.show(false)

    // OneHot编码，注意setDropLast设置为false
    val encoder = new OneHotEncoder().
      setInputCol("CategoryIndex").
      setOutputCol("CategoryVec").
      setDropLast(false)
    val encoded = encoder.transform(indexed)
    encoded.show(false)

    //将字段组合成向量feature
    val encodeDF: DataFrame = encoded
    val assembler = new VectorAssembler().
      setInputCols(Array("CategoryVec", "TotalValue")).
      setOutputCol("features")
    val vecDF: DataFrame = assembler.transform(encodeDF)
    vecDF.show(false)

    // 标准化--均值标准差
    val scaler = new StandardScaler().
      setInputCol("features").
      setOutputCol("scaledFeatures").
      setWithStd(true).
      setWithMean(true)
    val scalerModel = scaler.fit(vecDF)

    val scaledData: DataFrame = scalerModel.transform(vecDF)
    scaledData.show(false)


    sc.stop()
    spark.stop()
  }

}
