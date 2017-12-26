package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
/**
  * Created by sunlu on 17/12/26.
  * 参考链接：
  * Spark GraphX学习笔记
  * http://blog.csdn.net/qq_34531825/article/details/52324905
  *
  */
object myGraphX {
  def main(args:Array[String]){

    // Create the context
    val sparkConf = new SparkConf().setAppName("myGraphPractice").setMaster("local[2]")
    val sc=new SparkContext(sparkConf)

    // 顶点RDD[顶点的id,顶点的属性值]
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // 边RDD[起始点id,终点id，边的属性（边的标注,边的权重等）]
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // 默认（缺失）用户
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    //使用RDDs建立一个Graph（有许多建立Graph的数据来源和方法，后面会详细介绍）
    val graph = Graph(users, relationships, defaultUser)
  }

}
