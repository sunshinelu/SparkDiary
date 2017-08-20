package com.evayInfo.Inglory.Project.Recommend.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/20.
 */
object ItemCF {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"UserCF").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    //1 读取样本数据
    val data_path = "data/sample_itemcf2.txt"
    val data = sc.textFile(data_path)
    val rdd1 = data.map(_.split(",")).map(f => (MatrixEntry(f(0).toLong, f(1).toLong, f(2).toDouble))).cache()

    /**
     * 相似度.
     * @param itemId1 物品
     * @param itemId2 物品
     * @param similar 相似度
     */
    case class ItemSimi(
                         val itemId1: Long,
                         val itemId2: Long,
                         val similar: Double
                         ) extends Serializable

    //calculate item-item similarities
    val ratings = new CoordinateMatrix(rdd1) //.transpose()
    val itemSimi = ratings.toRowMatrix.columnSimilarities(0.1)
    val itemSimiRdd = itemSimi.entries.map(f => ItemSimi(f.i, f.j, f.value))
    itemSimiRdd.collect().foreach(println)
    /*
    ItemSimi(2,3,0.3333333333333334)
    ItemSimi(1,2,0.6666666666666669)
    ItemSimi(2,4,0.3333333333333334)
    ItemSimi(3,4,0.3333333333333334)
    ItemSimi(1,3,0.6666666666666669)

     */



    sc.stop()
    spark.stop()

  }

}
