package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.optimization.SquaredL2Updater

/**
  * Created by sunlu on 17/12/26.
  * 参考链接：
  * Spark机器学习系列之13： 支持向量机SVM
  * http://blog.csdn.net/qq_34531825/article/details/52881804
  *
  */
object mySVM {
  def main(args:Array[String]){
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf=new SparkConf().setMaster("local").setAppName("My App")
    val sc=new SparkContext(conf)

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "/data/mllib/sample_libsvm_data.txt")
    //println(data.collect()(0))//检查数据

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    /*
     * stepSize: 迭代步长，默认为1.0
     * numIterations: 迭代次数，默认为100
     * regParam: 正则化参数，默认值为0.0
     * miniBatchFraction: 每次迭代参与计算的样本比例，默认为1.0
     * gradient：HingeGradient ()，梯度下降；
     * updater：SquaredL2Updater ()，正则化，L2范数；
     * optimizer：GradientDescent (gradient, updater)，梯度下降最优化计算。
     */
    val svmAlg=new SVMWithSGD()
    svmAlg.optimizer
      .setNumIterations(100)
      .setRegParam(0.1)//正则化参数
      .setUpdater(new L1Updater)
    val modelL1=svmAlg.run(training)


    // Clear the default threshold.
    modelL1.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = modelL1.predict(point.features)
      (score, point.label)//return score and label
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC = " + auROC)

  }
}
