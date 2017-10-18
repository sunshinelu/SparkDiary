package com.evayInfo.Inglory.SparkDiary.graphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/18.
 */
object GraphXDemo2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    // bulid spark environment
    val sparkConf = new SparkConf().setAppName(s"GraphXDemo2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/followers.txt")
    println("graph.edges is: ")
    graph.edges.collect().foreach(println)
    println("graph.vertices is: ")
    graph.vertices.collect().foreach(println)

    println("graph.numEdges is: " + graph.numEdges)
    println("graph.vertices is: " + graph.vertices)
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    println("ranks is: ")
    ranks.collect().foreach(println)
    // Join the ranks with the usernames
    val users = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/users.txt").
      map { line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1))
      }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))


    // Find the connected components
    val cc = graph.connectedComponents().vertices
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))

    sc.stop()
    spark.stop()
  }

}
