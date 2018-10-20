package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 18/10/19.
 *
 * 功能：构建FPGrowth模型，并对输入的数据进行预测
 *
 * 输入：
 * ipt_table:输入表的表名，String类型
 * col_name:输入数据所在列的列名，String类型
 * sep:设置输入数据之间的分隔符，String类型
 * support:设置support参数，Double类型
 * confidence:设置confidence参数，Double类型
 * partitions:partitions数量，Int类型
 * opt_table:输出表的表名，String类型
 * model_path:模型保存的路径，String类型
 *
 * 输出：FPGrowth模型和预测结果
 */
class BuildFPGrowthModelTemp {

  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  // 链接mysql配置信息
  val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
    "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
  val user = "root"
  val password = "root"
  val prop = new Properties()
  prop.setProperty("user", user)
  prop.setProperty("password", password)

  def BuildFPGrowthModel(ipt_table:String, col_name:String,sep:String,support:Double,confidence:Double, partitions:Int,opt_table:String,model_path:String) = {

    val SparkConf = new SparkConf().setAppName(s"BuildFPGrowthModel:FPGrowthModel").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val ipt_df = spark.read.jdbc(url, ipt_table, prop).select(col_name)
    val ipt_rdd1 = ipt_df.rdd.map { case Row(x: String) => x }
    val ipt_rdd2 = ipt_rdd1.map(s => s.trim.split(' '))
    ipt_df.printSchema()
    ipt_df.show(truncate = false)

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
    fpg_model.save(sc, model_path)

    // predict function

    def rule_predict(ipt:String, sep:String):String ={
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

    def rule_predict2(ipt:String, sep:String):String ={
      val arr = ipt.split(sep).map(_.toString)
      val proposals = fpg_model.generateAssociationRules(minConfidence).map{ rule =>
        val antecedent = rule.antecedent
        val consequent = rule.consequent
        antecedent.flatMap{rule => if (rule.forall(x => arr.contains(x.toString))) {
          //          consequent.filter(x =>  !arr.contains(x.contains(x.toString))) // 使用该方法结果出错！
          consequent.filterNot(arr.contains)
        }else {
          Set.empty[String]
        }
        }}.reduce(_ ++ _).distinct.toSet.mkString(sep)

      proposals
    }

    def rule_predict3(ipt:String):String = {
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
      }.reduce(_ ++ _).distinct//.toSet.asInstanceOf[Array[String]]
      val result = proposals.mkString(sep)
      if (result != null) {
        result.toString
      } else "null"
    }

    def rule_pred_udf = udf((ipt:String) => rule_predict3(ipt).mkString(sep))

    // prediction
    //    val opt_df = ipt_df.withColumn("predict",rule_pred_udf($"items"))
    //    opt_df.cache()
    //    opt_df.printSchema()
    //    opt_df.show(truncate = false)
    //    opt_df.write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append

    val opt_df = ipt_rdd1.map{x => (x,rule_predict3(x.toString))}.toDF(col_name,"predict")
    opt_df.show()
    opt_df.write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append




    sc.stop()
    spark.stop()

  }

}
