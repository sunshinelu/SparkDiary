package com.evayInfo.Inglory.SparkDiary.graphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/18.
 */
object GraphXDemo3 {

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

    // Load my user data and parse into tuples of user id and attribute list
    val users = (sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/users.txt")
      .map(line => line.split(",")).map(parts => (parts.head.toLong, parts.tail)))

    // Parse the edge data which is already in userId -> userId format
    val followerGraph = GraphLoader.edgeListFile(sc, "file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/followers.txt")

    // Attach the user attributes
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
      case (uid, deg, None) => Array.empty[String]
    }

    // Restrict the graph to users with usernames and names
    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

    // Compute the PageRank
    val pagerankGraph = subgraph.pageRank(0.001)

    // Get the attributes of the top pagerank users
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }

    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))

    sc.stop()
    spark.stop()

  }
}
