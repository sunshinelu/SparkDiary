package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 18/10/12.
 *
 *
 * 测试数据为：
r z h k p
z y x w v u t s
s x o n r
x z y m t s q e
z
x z y r q t p
 *
 */
object FPGrowth_Test2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"FPGrowth_Test2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val minSupport = 0.2
    val minConfidence = 0.8
    val numPartitions = 10

    val dataset = spark.createDataset(Seq(
      "r z h k p",
      "z y x w v u t s",
      "s x o n r",
      "x z y m t s q e",
      "z",
      "x z y r q t p")
    ).toDF("items")

    val rdd1 = dataset.rdd.map { case Row(x: String) => x }
    val rdd2 = rdd1.map(s => s.trim.split(' '))

    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)
    val model = fpg.run(rdd2)

    //      val rdd3 = model.generateAssociationRules(minConfidence)
    //      rdd3.collect().map(x => x..toString()).toDF("").show()

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    /*

     */

    println("=====")

    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
        s"${rule.consequent .mkString("[", ",", "]")},${rule.confidence}")
    }
/*

 */

    val df1 = model.freqItemsets.map {x => (x.items.mkString(","), x.freq)}.toDF("items","freq")
    println("======= print freqItemsets =======")
    df1.show(truncate=false)
    /*
    +-------+----+
|  items|freq|
+-------+----+
|      z|   5|
|      x|   4|
|    x,z|   3|
|      y|   3|
|    y,x|   3|
|  y,x,z|   3|
|    y,z|   3|
|      r|   3|
|    r,x|   2|
|    r,z|   2|
|      s|   3|
|    s,y|   2|
|  s,y,x|   2|
|s,y,x,z|   2|
|  s,y,z|   2|
|    s,x|   3|
|  s,x,z|   2|
|    s,z|   2|
|      t|   3|
|    t,y|   3|
+-------+----+
     */

    val df2 =  model.generateAssociationRules(minConfidence).map{x =>
      (x.antecedent.mkString(","), x.consequent.mkString(","), x.confidence)}.toDF("antecedent", "consequent", "confidence")
    println("======= print generateAssociationRules ======")
    df2.show(truncate=false)
    /*
+----------+----------+----------+
|antecedent|consequent|confidence|
+----------+----------+----------+
|     t,s,y|         x|       1.0|
|     t,s,y|         z|       1.0|
|     y,x,z|         t|       1.0|
|         y|         x|       1.0|
|         y|         z|       1.0|
|         y|         t|       1.0|
|         p|         r|       1.0|
|         p|         z|       1.0|
|     q,t,z|         y|       1.0|
|     q,t,z|         x|       1.0|
|       q,y|         x|       1.0|
|       q,y|         z|       1.0|
|       q,y|         t|       1.0|
|     t,s,x|         y|       1.0|
|     t,s,x|         z|       1.0|
|   q,t,y,z|         x|       1.0|
|   q,t,x,z|         y|       1.0|
|       q,x|         y|       1.0|
|       q,x|         t|       1.0|
|       q,x|         z|       1.0|
+----------+----------+----------+
     */


    sc.stop()
    spark.stop()
  }

}
