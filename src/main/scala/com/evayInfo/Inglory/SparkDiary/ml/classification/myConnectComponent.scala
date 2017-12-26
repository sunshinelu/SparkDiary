package com.evayInfo.Inglory.SparkDiary.ml.classification
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD
/**
  * Created by sunlu on 17/12/26.
  * 参考链接：
  * Spark GraphX学习笔记
  * http://blog.csdn.net/qq_34531825/article/details/52324905
  *
  */

object myConnectComponent {
  def main(args:Array[String]){

    val sparkConf = new SparkConf().setAppName("myGraphPractice").setMaster("local[2]")
    val sc=new SparkContext(sparkConf)
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val graph=GraphLoader.edgeListFile(sc, "/spark-2.0.0-bin-hadoop2.6/data/graphx/followers.txt")

    graph.vertices.foreach(print)
    println
    graph.edges.foreach(print)
    println

    val cc=graph.connectedComponents().vertices
    cc.foreach(print)
    println
    /*输出结果
     *  (VertexId,cc)
     * (4,1)(1,1)(6,1)(3,1)(2,1)(7,1)
     */

    //强连通图-stronglyConnectedComponents
    val maxIterations=10//the maximum number of iterations to run for
    val cc2=graph.stronglyConnectedComponents(maxIterations).vertices
    cc2.foreach(print)
    println


    val path2="/spark-2.0.0-bin-hadoop2.6/data/graphx/users.txt"
    val users=sc.textFile(path2).map{//map 中包含多行 必须使用{}
      line=>val fields=line.split(",")
        (fields(0).toLong,fields(1))//(id,name) 多行书写 最后一行才是返回值 且与上行splitsplit(",")之间要有换行
    }
    users.collect().foreach { println}
    println
    /*输出结果 (VertexId,name)
     * (1,BarackObama)
     * (2,ladygaga)
     * ...
     */


    val joint=cc.join(users)
    joint.collect().foreach { println}
    println

    /*输出结果
     * (VertexId,(cc,name))
     * (4,(1,justinbieber))
     * (6,(3,matei_zaharia))
     */

//    val name_cc=joint.map{
//      case (VertexId,(cc,name))=>(name,cc)
//    }
//    name_cc.foreach(print)
    /*
     * (name,cc)
     * (BarackObama,1)(jeresig,3)(odersky,3)(justinbieber,1)(matei_zaharia,3)(ladygaga,1)
     */

  }


}
