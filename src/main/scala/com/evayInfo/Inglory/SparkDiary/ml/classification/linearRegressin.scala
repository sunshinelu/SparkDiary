package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
/**
  * Created by sunlu on 17/12/26.
  * 参考链接：
  * Spark2.0机器学习系列之12： 线性回归及L1、L2正则化区别与稀疏解
  * http://blog.csdn.net/qq_34531825/article/details/52689654
  *
  */

object linearRegressin {
  def main(args:Array[String]){
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val warehouseLocation = "file///:G:/Projects/Java/Spark/spark-warehouse"

    val spark=SparkSession
      .builder()
      .appName("myLinearRegression")
      .master("local[4]")
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .getOrCreate();
    val training=spark.read.format("libsvm").load("/data/mllib/sample_linear_regression_data.txt")

    val lr=new  LinearRegression()
      .setMaxIter(100)
      .setElasticNetParam(0.3)//alpha: 0-ridge;1-lasso;
      .setRegParam(0.5)//正则化参数lambda
    //.setSolver("l-bfgs")//default:l-bfgs,还可选sgd

    val lrModel=lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    //println(lr.explainParams())
  }
}
