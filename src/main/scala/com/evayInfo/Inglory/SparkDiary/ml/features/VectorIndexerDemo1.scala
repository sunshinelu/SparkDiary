package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/1/2.
 *
 * 参考链接：
 * spark ML 中 VectorIndexer, StringIndexer等用法
 * http://blog.csdn.net/shenxiaoming77/article/details/63715525
 *
 * 主要作用：提高决策树或随机森林等ML方法的分类效果。
VectorIndexer是对数据集特征向量中的类别（离散值）特征（index categorical features categorical features ）进行编号。
它能够自动判断那些特征是离散值型的特征，并对他们进行编号，具体做法是通过设置一个maxCategories，
特征向量中某一个特征不重复取值个数小于maxCategories，则被重新编号为0～K（K<=maxCategories-1）。
某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）。
 *
 */
object VectorIndexerDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"RandomForestDemo1").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val filePath = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sample_libsvm_data.txt"
    // Load and parse the data file, converting it to a DataFrame.
    val data = spark.read.format("libsvm").load(filePath)
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))
/*

 */

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()
/*

 */
/*
+-------------------------+-------------------------+
|features                 |indexedFeatures          |
+-------------------------+-------------------------+
|(3,[0,1,2],[2.0,5.0,7.0])|(3,[0,1,2],[2.0,1.0,1.0])|
|(3,[0,1,2],[3.0,5.0,9.0])|(3,[0,1,2],[3.0,1.0,2.0])|
|(3,[0,1,2],[4.0,7.0,9.0])|(3,[0,1,2],[4.0,3.0,2.0])|
|(3,[0,1,2],[2.0,4.0,9.0])|(3,[0,1,2],[2.0,0.0,2.0])|
|(3,[0,1,2],[9.0,5.0,7.0])|(3,[0,1,2],[9.0,1.0,1.0])|
|(3,[0,1,2],[2.0,5.0,9.0])|(3,[0,1,2],[2.0,1.0,2.0])|
|(3,[0,1,2],[3.0,4.0,9.0])|(3,[0,1,2],[3.0,0.0,2.0])|
|(3,[0,1,2],[8.0,4.0,9.0])|(3,[0,1,2],[8.0,0.0,2.0])|
|(3,[0,1,2],[3.0,6.0,2.0])|(3,[0,1,2],[3.0,2.0,0.0])|
|(3,[0,1,2],[5.0,9.0,2.0])|(3,[0,1,2],[5.0,4.0,0.0])|
+-------------------------+-------------------------+
结果分析：特征向量包含3个特征，即特征0，特征1，特征2。如Row=1,对应的特征分别是2.0,5.0,7.0.被转换为2.0,1.0,1.0。
我们发现只有特征1，特征2被转换了，特征0没有被转换。这是因为特征0有6中取值（2，3，4，5，8，9），多于前面的设置setMaxCategories(5)
，因此被视为连续值了，不会被转换。
特征1中，（4，5，6，7，9）-->(0,1,2,3,4,5)
特征2中,  (2,7,9)-->(0,1,2)

输出DataFrame格式说明（Row=1）：
3个特征 特征0，1，2      转换前的值
|(3,    [0,1,2],      [2.0,5.0,7.0])
3个特征 特征1，1，2       转换后的值
|(3,    [0,1,2],      [2.0,1.0,1.0])|
 */

    sc.stop()
    spark.stop()
  }
}
