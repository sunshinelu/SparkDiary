package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DataType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 18/10/19.
 */
object FPGrowth_Test6 {

  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    // 链接mysql配置信息
    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)


    val ipt_table = "FPGrowth_data"
    val col_name = "items"
    val sep = " "
    val support = 0.2
    val confidence = 0.8
    val partitions = 10
    val opt_table = "FPGrowth_data_pred"
    val model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/fpg_model"

    val SparkConf = new SparkConf().setAppName(s"BuildFPGrowthModel:FPGrowthModel").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val ipt_df_1 = spark.createDataset(Seq(
      "r z h k p",
      "z y x w v u t s",
      "s x o n r",
      "x z y m t s q e",
      "z",
      "x z y r q t p")
    ).toDF("items")
//    ipt_df_1.describe().show()
//    ipt_df_1.printSchema()
//    ipt_df_1.show(truncate = false)

    // 读取mysql数据
    val ipt_df = spark.read.jdbc(url, ipt_table, prop).na.drop().toDF(col_name)//.select(col_name)
//    ipt_df_2.printSchema()
//    ipt_df_2.show(truncate = false)
//    ipt_df_2.describe().show()

    val ipt_rdd1 = ipt_df.rdd.map { case Row(x: String) => x }
//    ipt_rdd1.collect().foreach(println)

    val ipt_rdd2 = ipt_rdd1.map(s => s.trim.split(sep))
    ipt_df.printSchema()
    ipt_df.show(truncate = false)
//    ipt_rdd2.collect().foreach(println)

    // 建模参数设置
    val minSupport = support // 0.2
    val minConfidence = confidence // 0.8
    val numPartitions = partitions // 10

    // 构建FPGrowth模型
    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)
    val fpg_model = fpg.run(ipt_rdd2)

    // save fpg_model
//    fpg_model.save(sc, model_path)

    def rule_predict3(ipt:String):String = {
      val items  = ipt.trim.split(sep)
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
      }.reduce(_ ++ _).distinct//.toSet.asInstanceOf[Array[String]]
      val result = proposals.mkString(sep)
      if (result != null) {
        result.toString
      } else "null"
    }
    val rule_pred_udf = udf((ipt:String) => rule_predict3(ipt).mkString(sep))



    def rule_pred3(ipt:String, sep:String):String = {
      val items  = ipt.trim.split(sep).toSet

      val proposals = fpg_model.generateAssociationRules(minConfidence).map{rule =>
        val antecedent = rule.antecedent.toSeq
        val consequent = rule.consequent.toSeq
        if (items != null) {
          //          val itemset = items//.toSet
          antecedent.flatMap(ant =>
            if (items != null && ant.forall(item => items.contains(item.toString))) {
              consequent.filter(item => !items.contains(item.toString))
            } else {
              Seq.empty[String]
            })//.distinct
        } else {
          Seq.empty[String]
        }
      }.reduce(_ ++ _).distinct
      val result = proposals.mkString(sep)
      result
    }

    for (i <- ipt_rdd1.collect()) {
      println("input: " + i)
      val t1 = rule_pred3(i, sep)
      println("output: " + t1)
    }

    println("=============")
//    ipt_rdd2.map{x => (x.mkString(sep), rule_pred3(x.mkString(sep), sep))}.collect().foreach(println)
    println("=============")



    ipt_df.persist()
    ipt_df.show(truncate = false)
    val rule_pred_udf2 = udf((ipt:String) => rule_pred3(ipt, " "),DataTypes.StringType)
    val train_df_pred = ipt_df.withColumn("pred",rule_pred_udf2($"items"))
    train_df_pred.show(truncate = false)

//    ipt_rdd2.map{x => (x.mkString(sep), rule_pred3(x.mkString(sep), sep))}


    // prediction
//    val opt_df = ipt_df.withColumn("predict",rule_pred_udf($"items"))
//    opt_df.cache()
//    opt_df.printSchema()
//    opt_df.show(truncate = false)
//    opt_df.write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append



    sc.stop()
    spark.stop()
  }
}
