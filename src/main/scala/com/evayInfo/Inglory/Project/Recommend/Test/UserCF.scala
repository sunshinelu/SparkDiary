package com.evayInfo.Inglory.Project.Recommend.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/20.
 * 运行成功，处理流程有问题，UserCF2为修正版！
 */
object UserCF {


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

    /**
     * 相似度.
     * @param userId1 物品
     * @param userId2 物品
     * @param similar 相似度
     */
    case class UserSimi(
                         val userId1: Long,
                         val userId2: Long,
                         val similar: Double
                         ) extends Serializable


    //1 读取样本数据
    val data_path = "data/sample_itemcf2.txt"
    val data = sc.textFile(data_path)
    val rdd1 = data.map(_.split(",")).map(f => (MatrixEntry(f(0).toLong, f(1).toLong, f(2).toDouble))).cache()


    //calculate similarities
    val ratings = new CoordinateMatrix(rdd1).transpose()
    val userSimi = ratings.toRowMatrix.columnSimilarities(0.1)

    // user-user similarity
    val userSimiRdd = userSimi.entries.map(f => UserSimi(f.i, f.j, f.value))
    userSimiRdd.collect().foreach(println)
    /*
    UserSimi(1,4,0.4999999999999999)
    UserSimi(2,3,0.4999999999999999)
    UserSimi(1,2,0.4999999999999999)
    UserSimi(3,6,0.7071067811865475)
    UserSimi(4,5,0.408248290463863)
    UserSimi(3,5,0.408248290463863)
    UserSimi(4,6,0.7071067811865475)
    UserSimi(2,5,0.816496580927726)
    UserSimi(1,5,0.816496580927726)
    UserSimi(3,4,0.4999999999999999)
     */
    // user1, user1, similar
    val rdd_app_R1 = userSimiRdd.map { f => (f.userId1, f.userId2, f.similar) } //.union(userSimiRdd.map { f => (f.userId2, f.userId1, f.similar) })
    println("rdd_app_R1 is: ")
    rdd_app_R1.collect().foreach(println)
    /*
(1,4,0.4999999999999999)
(2,3,0.4999999999999999)
(1,2,0.4999999999999999)
(3,6,0.7071067811865475)
(4,5,0.408248290463863)
(3,5,0.408248290463863)
(4,6,0.7071067811865475)
(2,5,0.816496580927726)
(1,5,0.816496580927726)
(3,4,0.4999999999999999)
     */

    // user item value
    val user_prefer1 = rdd1.map { f => (f.i, f.j, f.value) }
    println("user_prefer1 is: ")
    user_prefer1.collect().foreach(println)
    /*
(1,1,1.0)
(1,2,1.0)
(2,1,1.0)
(2,3,1.0)
(3,3,1.0)
(3,4,1.0)
(4,2,1.0)
(4,4,1.0)
(5,1,1.0)
(5,2,1.0)
(5,3,1.0)
(6,4,1.0)
     */

    // user1, [(user1, similar),(item, value)]
    val rdd_app_R2 = rdd_app_R1.map { f => (f._1, (f._2, f._3)) }.join(user_prefer1.map(f => (f._1, (f._2, f._3))))
    println("rdd_app_R2 is: ")
    rdd_app_R2.collect().foreach(println)
    /*
(4,((5,0.408248290463863),(2,1.0)))
(4,((5,0.408248290463863),(4,1.0)))
(4,((6,0.7071067811865475),(2,1.0)))
(4,((6,0.7071067811865475),(4,1.0)))
(2,((3,0.4999999999999999),(1,1.0)))
(2,((3,0.4999999999999999),(3,1.0)))
(2,((5,0.816496580927726),(1,1.0)))
(2,((5,0.816496580927726),(3,1.0)))
(1,((4,0.4999999999999999),(1,1.0)))
(1,((4,0.4999999999999999),(2,1.0)))
(1,((2,0.4999999999999999),(1,1.0)))
(1,((2,0.4999999999999999),(2,1.0)))
(1,((5,0.816496580927726),(1,1.0)))
(1,((5,0.816496580927726),(2,1.0)))
(3,((6,0.7071067811865475),(3,1.0)))
(3,((6,0.7071067811865475),(4,1.0)))
(3,((5,0.408248290463863),(3,1.0)))
(3,((5,0.408248290463863),(4,1.0)))
(3,((4,0.4999999999999999),(3,1.0)))
(3,((4,0.4999999999999999),(4,1.0)))
     */

    val rdd_app_R3 = rdd_app_R2.map { f => ((f._2._1._1, f._2._2._1), f._2._2._2 * f._2._1._2) }
    println("rdd_app_R3 is: ")
    rdd_app_R3.collect().foreach(println)
    /*
    ((5,2),0.408248290463863)
    ((5,4),0.408248290463863)
    ((6,2),0.7071067811865475)
    ((6,4),0.7071067811865475)
    ((3,1),0.4999999999999999)
    ((3,3),0.4999999999999999)
    ((5,1),0.816496580927726)
    ((5,3),0.816496580927726)
    ((4,1),0.4999999999999999)
    ((4,2),0.4999999999999999)
    ((2,1),0.4999999999999999)
    ((2,2),0.4999999999999999)
    ((5,1),0.816496580927726)
    ((5,2),0.816496580927726)
    ((6,3),0.7071067811865475)
    ((6,4),0.7071067811865475)
    ((5,3),0.408248290463863)
    ((5,4),0.408248290463863)
    ((4,3),0.4999999999999999)
    ((4,4),0.4999999999999999)
     */
    val rdd_app_R4 = rdd_app_R3.reduceByKey((x, y) => x + y)
    println("rdd_app_R4 is: ")
    rdd_app_R4.collect().foreach(println)
    /*
    ((3,1),0.4999999999999999)
    ((6,3),0.7071067811865475)
    ((5,4),0.816496580927726)
    ((2,1),0.4999999999999999)
    ((5,1),1.632993161855452)
    ((5,2),1.224744871391589)
    ((4,4),0.4999999999999999)
    ((4,2),0.4999999999999999)
    ((2,2),0.4999999999999999)
    ((4,1),0.4999999999999999)
    ((3,3),0.4999999999999999)
    ((4,3),0.4999999999999999)
    ((6,4),1.414213562373095)
    ((6,2),0.7071067811865475)
    ((5,3),1.224744871391589)
     */
    val rdd_app_R5 = rdd_app_R4.leftOuterJoin(user_prefer1.map(f => ((f._1, f._2), 1))).filter(f => f._2._2.isEmpty).map(f => (f._1._1, (f._1._2, f._2._1)))
    println("rdd_app_R5 is: ")
    rdd_app_R5.collect().foreach(println)
    /*

    (3,(1,0.4999999999999999))
    (6,(3,0.7071067811865475))
    (5,(4,0.816496580927726))
    (2,(2,0.4999999999999999))
    (4,(1,0.4999999999999999))
    (4,(3,0.4999999999999999))
    (6,(2,0.7071067811865475))
     */
    val rdd_app_R6 = rdd_app_R5.groupByKey()
    println("rdd_app_R6 is: ")
    rdd_app_R6.collect().foreach(println)
    /*
    (4,CompactBuffer((1,0.4999999999999999), (3,0.4999999999999999)))
    (6,CompactBuffer((3,0.7071067811865475), (2,0.7071067811865475)))
    (2,CompactBuffer((2,0.4999999999999999)))
    (3,CompactBuffer((1,0.4999999999999999)))
    (5,CompactBuffer((4,0.816496580927726)))
     */
    val r_number = 30
    val rdd_app_R7 = rdd_app_R6.map { f =>
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > r_number) i2_2.remove(0, (i2_2.length - r_number))
      (f._1, i2_2.toIterable)
    }
    println("rdd_app_R7 is: ")
    rdd_app_R7.collect().foreach(println)
    /*
    (4,ArrayBuffer((1,0.4999999999999999), (3,0.4999999999999999)))
    (6,ArrayBuffer((3,0.7071067811865475), (2,0.7071067811865475)))
    (2,ArrayBuffer((2,0.4999999999999999)))
    (3,ArrayBuffer((1,0.4999999999999999)))
    (5,ArrayBuffer((4,0.816496580927726)))

     */
    val rdd_app_R8 = rdd_app_R7.flatMap(f => {
      val id2 = f._2
      for (w <- id2) yield (f._1, w._1, w._2)
    }
    )
    println("rdd_app_R8 is: ")
    rdd_app_R8.collect().foreach(println)
    /*
    (4,1,0.4999999999999999)
    (4,3,0.4999999999999999)
    (6,3,0.7071067811865475)
    (6,2,0.7071067811865475)
    (2,2,0.4999999999999999)
    (3,1,0.4999999999999999)
    (5,4,0.816496580927726)
     */

    sc.stop()
    spark.stop()
  }


}
