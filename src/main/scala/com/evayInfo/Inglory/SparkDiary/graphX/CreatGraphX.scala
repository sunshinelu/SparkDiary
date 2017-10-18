package com.evayInfo.Inglory.SparkDiary.graphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/18.
 * GraphX 图形创建方式
 *
 * 测试数据：
 * 邮件交流网络图
 * https://snap.stanford.edu/data/email-Enron.html
 * email-Enron.txt.gz
 * Enron公司158名雇员的电子邮件往来数据构成一个邮件交流网络有向图
 *
 * 食品品味网络图
 * http://yongyeol.com/2011/12/15/paper-flavor-network.html
 * ingr_comp.zip
 * 通过三个食品网站获取得到的每个食品组成成分和每个成分对应的化学合成物构成一个网络
 * 个人社交网络图
 * http://snap.stanford.edu/data/egonets-Gplus.html
 * gplus.tar.gz
 * 数据中的用户圈子组成一个个人社交网络，数据集还包括个人属性信息
 */
object CreatGraphX {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    // bulid spark environment
    val sparkConf = new SparkConf().setAppName(s"GraphXDemo3").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    /*
    在GraphX里面有四种创建一个属性图的方法。每种构建图的方法对数据都有一定的格式要求。
     */

    /*
    方法1: 利用Object Graph 的方法创建
    Object Graph 是 Class Graph 的伴生对象，它定义了创建 Graph 对象的 apply 方法定义如下：
def apply[VD, ED](
  vertices: RDD[(VertexId, VD)],
  edges: RDD[Edge[ED]],
  defaultVertexAttr: VD = null
  ): Graph[VD, ED]
此方法通过传入顶点：RDD[(VertexId,VD)]和边：RDD[Edge[ED]] 就可以创建一个图。注意参数：
defaultVertexAttr 是用来设置那些边中的顶点不在传入的顶点集合当中的顶点的默认属性，所以这个值的类型必须是和传入顶点的属性的类型一样。
     */

    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Count all users which are postdocs
    graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    // Count all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId).count
    graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.take(5).foreach(println(_))


    /*

方法2: 利用 edgeListFile 创建
GraphLoader.edgeListFile函数来自动生成图，函数的定义如下：
def edgeListFile(
  sc: SparkContext,
  path: String,
  canonicalOrientation: Boolean = false,
  numEdgePartitions: Int = -1)
  : Graph[Int, Int]
sc、path 这两个参数不用多说，需要注意的参数解析如下：
path 指向包含边的文件或文件夹 要求：文件每一行用两个按照多个空格分割的正整数表示的边，如： scrId dstId，Spark 读取的时候会忽略# 开头的行
canonicalOrientation 表示图是否有方向 如果值为true，那么只会加载 srcId > dstId 的边，否则全部加载
加载完所有边之后，自动按照边生成顶点，默认的每个顶点的属性是1
numEdgePartitions 边分区个数默认是按照文件分区来划分的，也可以指定
下面看一下关键源码：
val edges = lines.mapPartitionsWithIndex { (pid, iter) =>  val builder = new EdgePartitionBuilder[Int, Int]
  iter.foreach { line =>    if (!line.isEmpty && line(0) != '#') {      val lineArray = line.split("\\s+")      if (lineArray.length < 2) {        throw new IllegalArgumentException("Invalid line: " + line)
      }      val srcId = lineArray(0).toLong      val dstId = lineArray(1).toLong      if (canonicalOrientation && srcId > dstId) {
        builder.add(dstId, srcId, 1)
      } else {
        builder.add(srcId, dstId, 1)
      }
    }
  }
}

     */

    // Load the edges as a graph
    val graph2 = GraphLoader.edgeListFile(sc, "file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/followers.txt")
    // Run PageRank
    val ranks2 = graph2.pageRank(0.0001).vertices
    println("ranks2 is: ")
    ranks2.take(5).foreach(println)
    // Join the ranks with the usernames
    val users2 = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/users.txt").
      map { line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1))
      }
    val ranksByUsername2 = users2.join(ranks2).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername2.take(5).mkString("\n"))


    /*

方法3：利用 fromEdges 创建
这个方法可以理解为edgeListFile方法内部就是调用这个方法。原理就是只根据边： RDD[Edge[ED]] 来生成图，顶点就是由所有构成边的顶点组成，顶点的默认属性用户可以指定，定义如下：
def fromEdges[VD: ClassTag, ED: ClassTag](
    edges: RDD[Edge[ED]],
    defaultValue: VD): Graph[VD, ED]
     */

    val iterations = 500
    val edgesRDD3 = sc.parallelize(Seq(Edge(1L, 3L, 4), Edge(2L, 4L, 2), Edge(3L, 4L, 1), Edge(3L, 4L, 1)))
    var g3 = Graph.fromEdges(edgesRDD3, 1)
    println("g3 is: ")
    g3.edges.take(5).foreach(println)

    //    var g = Graph.fromEdges(sc.makeRDD(
    //      Seq(Edge(1L, 3L, 1), Edge(2L, 4L, 1), Edge(3L, 4L, 1))), 1)
    for (i <- 1 to iterations) {
      //      println("Iteration: " + i)
      val newGraph: Graph[Int, Int] =
        g3.mapVertices((vid, vd) => (vd * i) / 17)
      g3 = g3.outerJoinVertices[Int, Int](newGraph.vertices) {
                                (vid, vd, newData) => newData.getOrElse(0)
}
    }


    /*
    方法4: fromEdgeTuples 创建
这个方法也可以理解为edgeListFile方法内部就是调用这个方法。原理就是只根据边： RDD[(VertexId, VertexId)] 来生成图，连边的属性都不知道，默认边的属性当然可以设置，顶点就是由所有构成边的顶点组成，顶点的默认属性用户可以指定，定义如下：
def fromEdgeTuples[VD](
  rawEdges: RDD[(VertexId, VertexId)],
  defaultValue: VD,
  uniqueEdges: Option[PartitionStrategy] = None)
  : Graph[VD, Int]
     */

    //    val edgesRDD4 = sc.parallelize(Seq(Edge(1L, 3L), Edge(2L, 4L), Edge(3L, 4L), Edge(3L, 4L)))
    //    var g4 = Graph.fromEdgeTuples(edgesRDD4, 1)


    sc.stop()
    spark.stop()
  }
}
