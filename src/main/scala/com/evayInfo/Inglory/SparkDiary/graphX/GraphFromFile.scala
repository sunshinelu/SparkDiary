package com.evayInfo.Inglory.SparkDiary.graphX

import breeze.numerics.abs
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.hashing.MurmurHash3

/**
 * Created by sunlu on 17/10/18.
 * 参考链接：
 * https://stackoverflow.com/questions/32396477/how-to-create-a-graph-from-a-csv-file-using-graph-fromedgetuples-in-spark-scala/32477121
 */
object GraphFromFile {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    // bulid spark environment
    val sparkConf = new SparkConf().setAppName(s"GraphFromFile").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    // read your file
    /*suppose your data is like
    v1 v3
    v2 v1
    v3 v4
    v4 v2
    v5 v3
    */

    /*
    方法1
     */
    val file = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/graphX/graphFile.txt")

    // create edge RDD of type RDD[(VertexId, VertexId)]
    val edgesRDD1: RDD[(VertexId, VertexId)] = file.map(line => line.split(" "))
      .map { line =>
        (MurmurHash3.stringHash(line(0)).toLong, MurmurHash3.stringHash(line(1)).toLong)
      }


    // create a graph
    val graph1 = Graph.fromEdgeTuples(edgesRDD1, 1)

    // you can see your graph
    graph1.triplets.collect.foreach(println)


    val edgesRDD2: RDD[(VertexId, VertexId)] = file.map(line => line.split(" "))
      .map { line =>
        (abs(line(0).hashCode.toLong), abs(line(1).hashCode.toLong))
      }


    // create a graph
    val graph2 = Graph.fromEdgeTuples(edgesRDD2, 1)

    // you can see your graph
    graph2.triplets.collect.foreach(println)




    sc.stop()
    spark.stop()
  }

}
