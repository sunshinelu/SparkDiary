package com.evayInfo.Inglory.SparkDiary.graphX

import breeze.linalg.SparseVector
import breeze.numerics.abs
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 * Created by sunlu on 17/10/19.
 */
object PeopleGraph {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  val projectDir = "/Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/graphX/gplus/"
  val id = "100129275726588145876"
  //只建立这个ID对应的社交关系图
  type Feature = breeze.linalg.SparseVector[Int]

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"CheckRecommTable").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext

    //通过 .feat 文件读取每个顶点的属性向量
    val featureMap = Source.fromFile(projectDir + id + ".feat").getLines().
      map {
        line =>
          val row = line.split(" ") //注意：ID 不能之间当作 Long 型的时候 常常用 hashcode 代替
        val key = abs(row.head.hashCode.toLong)
          val feat = SparseVector(row.tail.map(_.toInt))
          (key, feat)
      }.toMap //通过 .edges 文件得到两个用户之间的关系 并且计算他们相同特征的个数
    val edges = sc.textFile(projectDir + id + ".edges").map {
        line =>
          val row = line.split(" ")
          val srcId = abs(row(0).hashCode.toLong)
          val dstId = abs(row(1).hashCode.toLong)
          val srcFeat = featureMap(srcId)
          val dstFeat = featureMap(dstId)
          val numCommonFeats: Int = srcFeat dot dstFeat
          Edge(srcId, dstId, numCommonFeats)
      } //利用 fromEdges 建立图
    val egoNetwork = Graph.fromEdges(edges, 1) //查看一下具有3个相同特征的用户对
    println(egoNetwork.edges.filter(_.attr == 3).count())

    sc.stop()
    spark.stop()

  }
}
