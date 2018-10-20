package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.{FPGrowthModel, FPGrowth}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by sunlu on 18/10/20.
 * 测试同一份数据，有两种导入方式：一种从mysql中导入，另一种在spark中输入。
但是，使用同一个方法构建FPGrowth模型并进行预测时，从mysql中导入的数据报错，然而从spark中输入的方法不报错。
 *
 * How should I use the rules provided by FPGrowth in Scala?
 * https://stackoverflow.com/questions/48580819/how-should-i-use-the-rules-provided-by-fpgrowth-in-scala
 *
 */
object FPGrowth_Test7 {

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
    val opt_table_spark = "FPGrowth_data_pred_spark"
    val opt_table_mysql = "FPGrowth_data_pred_mysql"
    val model_path_mysql = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/fpg_model_mysql"
    val model_path_spark = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/fpg_model_spark"

    val SparkConf = new SparkConf().setAppName(s"FPGrowth_Test7").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    // load data from spark
    val df_spark = spark.createDataset(Seq(
      "r z h k p",
      "z y x w v u t s",
      "s x o n r",
      "x z y m t s q e",
      "z",
      "x z y r q t p")
    ).toDF(col_name)
    val rdd1_spark = df_spark.select(col_name).rdd.map { case Row(x: String) => x }
    val rdd2_spark = rdd1_spark.map(s => s.trim.split(sep))


    // 读取mysql数据
    val df_mysql = spark.read.jdbc(url, ipt_table, prop)
    val rdd1_mysql = df_mysql.select(col_name).rdd.map { case Row(x: String) => x }
    val rdd2_mysql = rdd1_mysql.map(s => s.trim.split(sep))


    // 建模参数设置
    val minSupport = support // 0.2
    val minConfidence = confidence // 0.8
    val numPartitions = partitions // 10

    // 构建FPGrowth模型
    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)

    // using rdd2_spark build fpg_model_spark model
    val fpg_model_spark = fpg.run(rdd2_spark)
    // save fpg_model_spark model
//    fpg_model_spark.save(sc, model_path_spark)

    // using rdd2_mysql build fpg_model_mysql model
    val fpg_model_mysql = fpg.run(rdd2_mysql)
    // save fpg_model_spark model
//    fpg_model_mysql.save(sc, model_path_mysql)


    // load fpg_model
    val fpg_model_spark_load = FPGrowthModel.load(sc, model_path_spark)
    val fpg_model_mysql_load = FPGrowthModel.load(sc, model_path_mysql)


    // define prediction function


/*
    val rules_spark_df =  fpg_model_spark_load.generateAssociationRules(minConfidence).map{x =>
      (x.antecedent, x.consequent, x.confidence)}.toDF("antecedent", "consequent", "confidence")
    val rules_myslq_df =  fpg_model_mysql_load.generateAssociationRules(minConfidence).map{x =>
      (x.antecedent, x.consequent, x.confidence)}.toDF("antecedent", "consequent", "confidence")

    rules_myslq_df

    val seq1 = Seq.empty[String]
    seq1

    def predic_Func_00(associationRules:DataFrame,ipt: String, sep: String) = {
      val items = ipt.trim.split(sep).toSet

      val rules: Array[(Seq[String], Seq[String])] = associationRules.select("antecedent", "consequent")
        .rdd.map(r => (r.getSeq(0), r.getSeq(1)))
        .collect().asInstanceOf[Array[(Seq[String], Seq[String])]]
      val brRules = sc.broadcast(rules)

      val proposals = if (items != null) {
        brRules.value.flatMap(rule =>
          if (items != null && rule._1.forall(item => items.contains(item.toString))) {
            val t1 = rule._2.filter(item => !items.contains(item.toString)).toSeq
            t1
          } else {
            seq1
          }) //.distinct
      } else {
        seq1
      }.reduce(_ ++ _).distinct.toArray
    //.reduce(_ ++ _).distinct
    val result = proposals.mkString(sep)

    }

    // define udf function
    val pred_spark_udf00 = udf((col: String) => predic_Func_00(rules_spark_df, col, sep))
    val pred_mysql_udf00 = udf((col: String) => predic_Func_00(rules_myslq_df, col, sep))

    // using predic_Func_0 predict rules
    val df_spark_pred00 = df_spark.withColumn("prediction", pred_spark_udf00(col(col_name)))
    println("========= df_spark_pred00 show =============")
    df_spark_pred00.show(truncate = false)

    val df_mysql_pred00 = df_mysql.withColumn("prediction", pred_mysql_udf00(col(col_name)))
    println("========= df_mysql_pred00 show =============")
    df_mysql_pred00.show(truncate = false)


//////////////////////////////////////////////



    val rules_spark: Seq[(Seq[String], Seq[String])] = fpg_model_spark_load.generateAssociationRules(minConfidence).map { rule =>
      val antecedent = rule.antecedent.toSeq.asInstanceOf[Seq[String]]//asInstanceOf[Array[String]]
    val consequent = rule.consequent.toSeq.asInstanceOf[Seq[String]]
      (antecedent, consequent)
    }.collect().toSeq.asInstanceOf[Seq[(Seq[String], Seq[String])]]

    val rules_mysql: Seq[(Seq[String], Seq[String])] = fpg_model_mysql_load.generateAssociationRules(minConfidence).map { rule =>
      val antecedent = rule.antecedent.toSeq.asInstanceOf[Seq[String]]//asInstanceOf[Array[String]]
    val consequent = rule.consequent.toSeq.asInstanceOf[Seq[String]]
      (antecedent, consequent)
    }.collect().toSeq.asInstanceOf[Seq[(Seq[String], Seq[String])]]

    def predic_Func_0(rules: Seq[(Seq[String], Seq[String])] , ipt: String, sep: String): String = {
      val items = ipt.split(sep).toSeq

      val proposals = rules.flatMap(rule =>
        if (items != null && rule._1.forall(item => items.contains(item.toString))) {
          rule._2.filter(item => !items.contains(item.toString)).toSeq.asInstanceOf[Seq[String]]
        } else {
          Seq.empty[String]
        }).reduce(_ ++ _).toSet

      val pred = proposals.filter(x => !items.contains(x)).mkString(sep)
      pred
    }
    // define udf function
    val pred_spark_udf0 = udf((col: String) => predic_Func_0(rules_spark, col, sep))
    val pred_mysql_udf0 = udf((col: String) => predic_Func_0(rules_mysql, col, sep))

    // using predic_Func_0 predict rules
    val df_spark_pred0 = df_spark.withColumn("prediction", pred_spark_udf0(col(col_name)))
    println("========= df_spark_pred0 show =============")
    df_spark_pred0.show(truncate = false)

    val df_mysql_pred0 = df_mysql.withColumn("prediction", pred_mysql_udf0(col(col_name)))
    println("========= df_mysql_pred0 show =============")
    df_mysql_pred0.show(truncate = false)

*/

    def predic_Func_1(fpg_model: FPGrowthModel[_], ipt: String, sep: String): String = {
      val arr = ipt.split(sep)
      val proposals = fpg_model.generateAssociationRules(minConfidence).map { rule =>
        val antecedent = rule.antecedent.toSeq.asInstanceOf[Seq[String]]//asInstanceOf[Array[String]]
        val consequent = rule.consequent.toSeq.asInstanceOf[Seq[String]]
        antecedent.flatMap { rule => if (rule.toString.forall(x => arr.contains(x.toString))) {
          consequent.toSet
        } else {
          Set.empty[String]
        }
        }
      }.reduce(_ ++ _).toSet
      val pred = proposals.filter(x => !arr.contains(x)).mkString(sep)
      pred
    }
    // define udf function
    val pred_spark_udf1 = udf((col: String) => predic_Func_1(fpg_model_spark_load, col, sep))
    val pred_mysql_udf1 = udf((col: String) => predic_Func_1(fpg_model_mysql_load, col, sep))

    // using predic_Func_1 predict rules
    val df_spark_pred1 = df_spark.withColumn("prediction", pred_spark_udf1(col(col_name)))
    println("========= df_spark_pred1 show =============")
    df_spark_pred1.show(truncate = false)

    val df_mysql_pred1 = df_mysql.withColumn("prediction", pred_mysql_udf1(col(col_name)))
    println("========= df_mysql_pred1 show =============")
    df_mysql_pred1.show(truncate = false)






    def predic_Func_2(fpg_model: FPGrowthModel[_], ipt: String, sep: String): String = {
      val arr = ipt.split(sep)
      val proposals = fpg_model.generateAssociationRules(minConfidence).map { rule =>
        val antecedent = rule.antecedent.toSeq.asInstanceOf[Seq[String]]
        val consequent = rule.consequent.toSeq.asInstanceOf[Seq[String]]
        antecedent.flatMap { rule => if (rule.forall(x => arr.contains(x.toString))) {
          //          consequent.filter(x =>  !arr.contains(x.contains(x.toString)))
          consequent.filterNot(arr.contains)
        } else {
          Set.empty[String]
        }
        }
      }.reduce(_ ++ _).distinct.toSet.mkString(sep)
      //      val pred = proposals.filter(x => !arr.contains(x)).mkString(sep)
      proposals
    }

    // define udf function
    val pred_spark_udf2 = udf((col: String) => predic_Func_1(fpg_model_spark_load, col, sep))
    val pred_mysql_udf2 = udf((col: String) => predic_Func_1(fpg_model_mysql_load, col, sep))

    // using predic_Func_1 predict rules
    val df_spark_pred2 = df_spark.withColumn("prediction", pred_spark_udf2(col(col_name)))
    println("========= df_spark_pred2 show =============")
    df_spark_pred2.show(truncate = false)

    val df_mysql_pred2 = df_mysql.withColumn("prediction", pred_mysql_udf2(col(col_name)))
    println("========= df_mysql_pred2 show =============")
//    df_mysql_pred2.show(truncate = false)


    def predic_Func_3(fpg_model: FPGrowthModel[_], ipt: String, sep: String): String = {
      val items = ipt.trim.split(sep).toSet

      val proposals = fpg_model.generateAssociationRules(minConfidence).map { rule =>
        val antecedent = rule.antecedent.toSeq.asInstanceOf[Seq[String]]
        val consequent = rule.consequent.toSeq.asInstanceOf[Seq[String]]
        if (items != null) {
          //          val itemset = items//.toSet
          antecedent.flatMap(ant =>
            if (items != null && ant.forall(item => items.contains(item.toString))) {
              consequent.filter(item => !items.contains(item.toString))
            } else {
              Seq.empty[String]
            }) //.distinct
        } else {
          Seq.empty[String]
        }
      }.reduce(_ ++ _).distinct
      val result = proposals.mkString(sep)
      result
    }

    // define udf function
    val pred_spark_udf3 = udf((col: String) => predic_Func_3(fpg_model_spark_load, col, sep))
    val pred_mysql_udf3 = udf((col: String) => predic_Func_3(fpg_model_mysql_load, col, sep))

    // using predic_Func_1 predict rules
    val df_spark_pred3 = df_spark.withColumn("prediction", pred_spark_udf3(col(col_name)))
    println("========= df_spark_pred3 show =============")
    df_spark_pred3.show(truncate = false)

    val df_mysql_pred3 = df_mysql.withColumn("prediction", pred_mysql_udf3(col(col_name)))
    println("========= df_mysql_pred3 show =============")
//    df_mysql_pred3.show(truncate = false)

    sc.stop()
    spark.stop()

  }

}
