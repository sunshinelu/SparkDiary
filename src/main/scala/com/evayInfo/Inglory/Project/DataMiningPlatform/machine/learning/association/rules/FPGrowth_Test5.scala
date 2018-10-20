package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, SparkSession}


/**
 * Created by sunlu on 18/10/19.
 */
object FPGrowth_Test5 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"FPGrowth_Test5").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val train_df = spark.createDataset(Seq(
      "r z h k p",
      "z y x w v u t s",
      "s x o n r",
      "x z y m t s q e",
      "z",
      "x z y r q t p")
    ).toDF("items")

    val test_df = spark.createDataset(Seq(
      "r z",
      "z y x",
      "s x o n r t",
      "x z y n",
      "n",
      "x")
    ).toDF("items")

    /*
    // 链接mysql配置信息
    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    val ipt_table = "FPGrowth_data"
    val train_df = spark.read.jdbc(url, ipt_table, prop)
*/

    val rdd1 = train_df.rdd.map { case Row(x: String) => x }
    val rdd2 = rdd1.map(s => s.trim.split(' '))
    val df1 = train_df.withColumn("items_arr", split($"items"," "))
    df1.printSchema()

    val minSupport = 0.2
    val minConfidence = 0.8
    val numPartitions = 10

    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)
    val fpg_model = fpg.run(rdd2)

    /*
    val test_arr = Array("r", "z", "h", "k", "p")
    val proposals = fpg_model.generateAssociationRules(minConfidence).map{ rule =>
      val antecedent = rule.antecedent//.asInstanceOf[Seq[String]]
    val consequent = rule.consequent//.asInstanceOf[Seq[String]]
//      println("antecedent is: " + antecedent.mkString("*"))
//      println("consequent is: " + consequent.mkString(" &&"))
      antecedent.flatMap{rule => if (rule.forall(x => test_arr.contains(x.toString))) {
        consequent.toSet
      }else {
        Set.empty[String]
      }
      }}.reduce(_ ++ _).distinct//.toSet
//      if (antecedent.forall(x => test_arr.contains(x.toString))) {
//        consequent.toSet
//      } else {
//        Set.empty[String]
//      }
//    }.reduce(_ ++ _)
    val test_arr_pred = proposals.filter(x => !test_arr.contains(x)).mkString(" ")
    println("==========")
    println(test_arr_pred)

*/

    def rule_pred(ipt:String, sep:String):String ={
      val arr = ipt.split(sep)
      val proposals = fpg_model.generateAssociationRules(minConfidence).map{ rule =>
        val antecedent = rule.antecedent//.asInstanceOf[Seq[String]]
        val consequent = rule.consequent//.asInstanceOf[Seq[String]]
        antecedent.flatMap{rule => if (rule.forall(x => arr.contains(x.toString))) {
          consequent.toSet
        }else {
          Set.empty[String]
        }
        }}.reduce(_ ++ _).toSet
      val pred = proposals.filter(x => !arr.contains(x)).mkString(sep)
      pred
    }

    def rule_pred2(ipt:String, sep:String):String ={
      val arr = ipt.split(sep)
      val proposals = fpg_model.generateAssociationRules(minConfidence).map{ rule =>
        val antecedent = rule.antecedent
        val consequent = rule.consequent
        antecedent.flatMap{rule => if (rule.forall(x => arr.contains(x.toString))) {
//          consequent.filter(x =>  !arr.contains(x.contains(x.toString)))
          consequent.filterNot(arr.contains)
        }else {
          Set.empty[String]
        }
        }}.reduce(_ ++ _).distinct.toSet.mkString(sep)
//      val pred = proposals.filter(x => !arr.contains(x)).mkString(sep)
      proposals
    }


    def rule_pred3(ipt:String, sep:String):String = {
      val items  = ipt.split(sep)

      val proposals = fpg_model.generateAssociationRules(minConfidence).map{rule =>
        val antecedent = rule.antecedent
        val consequent = rule.consequent
        if (items != null) {
//          val itemset = items//.toSet
          antecedent.flatMap(ant =>
            if (items != null && ant.forall(item => items.contains(item.toString))) {
              consequent.filter(item => !items.contains(item))
            } else {
              Array.empty[String]
            })//.distinct
        } else {
          Array.empty[String]
        }
      }.reduce(_ ++ _).distinct
      proposals.mkString(sep)
    }





    val sep = " "
    val rule_pred_udf = udf((ipt:String) => rule_pred3(ipt, sep))
    val train_df_pred = train_df.withColumn("pred",rule_pred_udf($"items"))
    train_df_pred.show(truncate = false)
    /*

+---------------+---------+
|items          |pred     |
+---------------+---------+
|r z h k p      |x y t    |
|z y x w v u t s|p r      |
|s x o n r      |y t p z  |
|x z y m t s q e|p r      |
|z              |x y t p r|
|x z y r q t p  |         |
+---------------+---------+

spark 2.3.2中的方法预测结果为：
+---------------+--------------------+----------+
|          items|           items_arr|prediction|
+---------------+--------------------+----------+
|      r z h k p|     [r, z, h, k, p]| [x, t, y]|
|z y x w v u t s|[z, y, x, w, v, u...|        []|
|      s x o n r|     [s, x, o, n, r]| [z, t, y]|
|x z y m t s q e|[x, z, y, m, t, s...|        []|
|              z|                 [z]| [x, t, y]|
|  x z y r q t p|[x, z, y, r, q, t...|       [s]|
+---------------+--------------------+----------+


     */




//    val antecedent = Array("q", "t", "y", "z")
//    antecedent.forall(x => test_arr.contains(x))
//    antecedent.map(rule => rule.forall(x => test_arr.contains(x.toString)))
//    antecedent.flatMap(rule => if (rule.forall(x => test_arr.contains(x.toString))) {
//
//    })

    /*
    val brRules = sc.broadcast(rules)

    val dt = df1.select($"items_arr").dataType


    val predictUDF = udf((items: Seq[String]) => {
      if (items != null) {
        val itemset = items.toSet
        brRules.value.flatMap(rule =>
          if (items != null && rule._1.forall(item => itemset.contains(item))) {
            rule._2.filter(item => !itemset.contains(item))
          } else {
            Seq.empty
          }).distinct
      } else {
        Seq.empty
      }})

    val df2 = df1.withColumn("pred", predictUDF($"items_arr"))
    df2.printSchema()
    df2.show(truncate = false)
*/


    sc.stop()
    spark.stop()
  }
}
