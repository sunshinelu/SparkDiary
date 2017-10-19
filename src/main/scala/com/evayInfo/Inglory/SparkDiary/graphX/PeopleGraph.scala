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
 * 个人社交网络图
 * http://snap.stanford.edu/data/egonets-Gplus.html
 * gplus.tar.gz
 *
 * 数据集是使用上面介绍的Google+提供的个人关系数据，解压之后有792个文件，每一个文件名去掉后缀代表的是网络ID，每个网络ID有6个文件，所以这里有132个个人关系网络。
 * 下面以ID为100129275726588145876的网络说明一下每个文件的含义：
 * .edges 记录的是边，即ID对应的用户之间有关联。
 * .feat 记录的是每个用户ID对应的特征，每个维度上面都是取值为 0 1。
 * .featnames 记录的是上面feat每个维度对应的含义（注意：上面之所以每个维度取值都是 0 1， 是因为这里的特征都是分类变量，并且做了 1 of n 编码）
 *
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
  //"100129275726588145876"
  //只建立这个ID对应的社交关系图
  type Feature = breeze.linalg.SparseVector[Int]

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"PeopleGraph").setMaster("local[*]").set("spark.executor.memory", "2g")
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
    val edges = sc.textFile("file://" + projectDir + id + ".edges").map {
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
