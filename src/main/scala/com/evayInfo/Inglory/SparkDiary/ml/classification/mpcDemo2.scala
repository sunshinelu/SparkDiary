package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/12/27.
 * 多层感知器模型参数优化
 */
object mpcDemo2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"mpcDemo2").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val filePath = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sample_multiclass_classification_data.txt"

    //加载数据,randomSplit时加了一个固定的种子seed=100，
    //是为了得到可重复的结果，方便调试算法，实际工作中不能这样设置
    val split  = spark.read.format("libsvm").load(filePath).randomSplit(Array[Double](0.6, 0.4), 100)
    val training = split(0)
    val test = split(1)
    training.show(100, false)

    //第一层树特征个数：4
    //最后一层，即输出层是labels个数（类数）:3
    //隐藏层自己定义
    val layer: Array[Int] = Array[Int](4, 6, 4, 3)
    val maxIter: Array[Int] = Array[Int](5, 10, 20, 50, 100, 200)
    val accuracy: Array[Double] = Array[Double](0, 0, 0, 0, 0, 0, 0, 0, 0, 0)


    //利用如下类似的循环可以很方便的对各种参数进行调优
    for (i <- 0 to maxIter.length - 1){
      val multilayerPerceptronClassifier = new MultilayerPerceptronClassifier().
        setLabelCol("label").
        setFeaturesCol("features").
        setLayers(layer).
        setMaxIter(maxIter(i)).
        setBlockSize(128).
        setSeed(1000)
      val model = multilayerPerceptronClassifier.fit(training)

      val predictions = model.transform(test)
      val evaluator = new MulticlassClassificationEvaluator().
        setLabelCol("label").
        setPredictionCol("prediction").
        setMetricName("accuracy")
      accuracy(i) = evaluator.evaluate(predictions)
    }

    //一次性输出所有评估结果
    for(j <- 0 to maxIter.length -1 ){
//      val str_accuracy = String.format(" accuracy =  %.2f", accuracy(j))
      val str_accuracy = accuracy(j)
//      val str_maxIter = String.format(" maxIter =  %d", maxIter(j))
      val str_maxIter = maxIter(j)
      println("str_maxIter = " + str_maxIter + "; str_accuracy = " + str_accuracy)
    }
    /*
 str_maxIter = 5; str_accuracy = 0.35384615384615387
str_maxIter = 10; str_accuracy = 0.7384615384615385
str_maxIter = 20; str_accuracy = 0.9076923076923077
str_maxIter = 50; str_accuracy = 0.9230769230769231
str_maxIter = 100; str_accuracy = 0.9230769230769231
str_maxIter = 200; str_accuracy = 0.9230769230769231
     */


    sc.stop()
    spark.stop()

  }

}
