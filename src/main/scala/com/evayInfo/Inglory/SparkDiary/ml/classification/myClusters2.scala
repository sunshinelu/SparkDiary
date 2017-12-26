package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.PowerIterationClustering

/**
  * Created by sunlu on 17/12/26.
  * 参考链接：
  * Spark2.0机器学习系列之11： 聚类(幂迭代聚类， power iteration clustering， PIC)
  * http://blog.csdn.net/qq_34531825/article/details/52675182
  */
object myClusters2 {
  //产生一个分布在圆形上的数据模型（见程序后面的图）：参数为半径和点数
  def generateCircle(radius: Double, n: Int): Seq[(Double, Double)] = {
    Seq.tabulate(n) { i =>
      val theta = 2.0 * math.Pi * i / n
      (radius * math.cos(theta), radius * math.sin(theta))
    }
  }

  //产生同心圆样的数据模型（见程序后面的图）：参数为同心圆的个数和点数
  //第一个同心圆上有nPoints个点，第二个有2*nPoints个点，第三个有3*nPoints个点，以此类推
  def generateCirclesRdd(
                          sc: SparkContext,
                          nCircles: Int,
                          nPoints: Int): RDD[(Long, Long, Double)] = {
    val points = (1 to nCircles).flatMap { i =>
      generateCircle(i, i * nPoints)
    }.zipWithIndex
    val rdd = sc.parallelize(points)
    val distancesRdd = rdd.cartesian(rdd).flatMap { case (((x0, y0), i0), ((x1, y1), i1)) =>
      if (i0 < i1) {
        Some((i0.toLong, i1.toLong, gaussianSimilarity((x0, y0), (x1, y1))))
      } else {
        None
      }
    }
    distancesRdd
  }

  /**
    * Gaussian Similarity
    */
  def gaussianSimilarity(p1: (Double, Double), p2: (Double, Double)): Double = {
    val ssquares = (p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p1._2 - p2._2)
    math.exp(-ssquares / 2.0)
  }


  def main(args:Array[String]){

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val warehouseLocation = "/Spark/spark-warehouse"

    val spark=SparkSession
      .builder()
      .appName("myClusters")
      .master("local[4]")
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .getOrCreate();

    val sc=spark.sparkContext

    // 产生测试数据
    val circlesRdd = generateCirclesRdd(sc, 2, 4)//产生具有2个半径不同的同心圆模型的相似度矩阵，第一个圆上有4个点，第二个圆上有2*4=8个点
    //circlesRdd.take(100).foreach(p=>print(p+"\n"))
    /**
      * circlesRdd数据格式实例：(srcId, dstId, similarity)
      * (0,1,0.22313016014842987)
      * (0,2,0.22313016014842973)
      * (1,2,0.22313016014842987)
      */

    /**PowerIterationClustering函数输入输出说明
      *
      *  It takes an RDD of (srcId, dstId, similarity) tuples and outputs a model
      *  with the clustering assignments. The similarities must be nonnegative.
      *  PIC assumes that the similarity measure is symmetric. A pair (srcId, dstId)
      *  regardless of the ordering should appear at most once in the input data.
      *  If a pair is missing from input, their similarity is treated as zero.
      *  它的输入circlesRdd是(srcId, dstId,similarity) 三元组的RDD（起始点->终点，相似度为边权重）。
      *  显然算法中选取的相似度函数是非负的，而且PIC 算法假设相似函数还满足对称性（边权重代表相似度，非负）。
      *  输入数据中（srcId,dstId）对不管先后顺序如何只能出现至多一次（即是无向图，两顶点边权重是唯一的）
      *  如果输入数据中不含这个数据对（不考虑顺序），则这两个数据对的相似度为0（没有这条边）.
      *
      */
    val modelPIC = new PowerIterationClustering()
      .setK(2)// k : 期望聚类数
      .setMaxIterations(40)//幂迭代最大次数
      .setInitializationMode("degree")//模型初始化，默认使用”random” ，即使用随机向量作为初始聚类的边界点，可以设置”degree”（就是图论中的度）。
      .run(circlesRdd)


    //输出聚类结果
    val clusters = modelPIC.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val assignmentsStr = assignments
      .map { case (k, v) =>
        s"$k -> ${v.sorted.mkString("[", ",", "]")}"
      }.mkString(", ")
    val sizesStr = assignments.map {
      _._2.length
    }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")
    /*
     * Cluster assignments: 1 -> [4,6,8,10], 0 -> [0,1,2,3,5,7,9,11]
     * cluster sizes: (4,8)
     */

  }

}
