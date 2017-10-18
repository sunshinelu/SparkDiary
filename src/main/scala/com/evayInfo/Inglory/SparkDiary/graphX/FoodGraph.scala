package com.evayInfo.Inglory.SparkDiary.graphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/18.
 */

//定义下面的类将 ingredient 和 compount 统一表示 注意父类一定要可以序列化
class FoodNode(val name: String) extends Serializable

case class Ingredient(override val name: String, val cat: String) extends FoodNode(name)

case class Compound(override val name: String, val cas: String) extends FoodNode(name)

object FoodGraph {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    SetLogger

    // bulid spark environment
    val sparkConf = new SparkConf().setAppName(s"FoodGraph").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val ingredients: RDD[(VertexId, FoodNode)] = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/graphX/ingr_comp/ingr_info.tsv").
      filter {
        !_.startsWith("#")
      }.map {
      line => val array = line.split("\t")
        (array(0).toLong, Ingredient(array(1), array(2)))
    }
    //获取得到最大的 ingredient 的ID 并且加1

    val maxIngrId = ingredients.keys.max() + 1
    val compounds: RDD[(VertexId, FoodNode)] = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/graphX/ingr_comp/comp_info.tsv").
      filter {
        !_.startsWith("#")
      }.map {
      line => val array = line.split("\t")
        (maxIngrId + array(0).toLong, Compound(array(1), array(2)))
    }
    //根据文件 ingr_comp.csv 生成边，注意第二列的所有顶点都要加上 maxIngrId

    val links = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/graphX/ingr_comp/ingr_comp.tsv").
      filter {
        !_.startsWith("#")
      }.map {
      line => val array = line.split("\t")
        Edge(array(0).toLong, maxIngrId + array(1).toLong, 1)
    }
    //将两个顶点合并
    val vertices = ingredients ++ compounds
    val foodNetWork = Graph(vertices, links)
    //foodNetWork.vertices.take(10).foreach(println)
    //访问一下这个网络前面5条triplet的对应关系
    foodNetWork.triplets.take(5).foreach(showTriplet _ andThen println _)

    sc.stop()
    spark.stop()
  }

  def showTriplet(t: EdgeTriplet[FoodNode, Int]): String = "The ingredient " ++ t.srcAttr.name ++ " contains " ++ t.dstAttr.name


}
