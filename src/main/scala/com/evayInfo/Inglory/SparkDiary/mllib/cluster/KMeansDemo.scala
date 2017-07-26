package com.evayInfo.Inglory.SparkDiary.mllib.cluster

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}

/**
  * Created by sunlu on 17/6/23.
  */
object KMeansDemo {
  def main(args: Array[String]) {
    //1 构建Spark对象
    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据1，格式为LIBSVM format
    val data = sc.textFile("data/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // 新建KMeans聚类模型，并训练
    val initMode = "k-means||"
    val numClusters = 4
    val numIterations = 100
    val model = new KMeans().
      setInitializationMode(initMode).
      setK(numClusters).
      setMaxIterations(numIterations).
      run(parsedData)
    val centers = model.clusterCenters
    println("centers")
    for (i <- 0 to centers.length - 1) {
      println(centers(i)(0) + "\t" + centers(i)(1))
    }

    // 误差计算
    val WSSSE = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    /*
        //保存模型
        val ModelPath = "result/KMeans_Model"
        model.save(sc, ModelPath)
        val sameModel = KMeansModel.load(sc, ModelPath)
        */
  }
}
