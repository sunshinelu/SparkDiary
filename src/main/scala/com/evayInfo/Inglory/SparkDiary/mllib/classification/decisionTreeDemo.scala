package com.evayInfo.Inglory.SparkDiary.mllib.classification

import org.apache.log4j.{Logger, Level}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by sunlu on 17/6/23.
  */
object decisionTreeDemo {
  def main(args: Array[String]) {
    //1 构建Spark对象
    val conf = new SparkConf().setAppName("DecisionTree").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据1，格式为LIBSVM format
    val data = MLUtils.loadLibSVMFile(sc, "data/sample_libsvm_data.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // 新建决策树
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // 误差计算
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val print_predict = labelAndPreds.take(20)
    println("label" + "\t" + "prediction")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

    /*
    // 保存模型
    val ModelPath = "result/Decision_Tree_Model"
    model.save(sc, ModelPath)
    val sameModel = DecisionTreeModel.load(sc, ModelPath)
    */
  }
}
