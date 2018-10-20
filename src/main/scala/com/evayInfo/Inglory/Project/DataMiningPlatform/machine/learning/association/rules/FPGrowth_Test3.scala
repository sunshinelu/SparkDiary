package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.{FPGrowthModel, FPGrowth}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.bytedeco.javacpp.hdf5.DataSet

/**
 * Created by sunlu on 18/10/19.
 */
object FPGrowth_Test3 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"FPGrowth_Test3").setMaster("local[*]").set("spark.executor.memory", "2g")
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

    val rdd1 = train_df.rdd.map { case Row(x: String) => x }
    val rdd2 = rdd1.map(s => s.trim.split(' '))
    val df1 = train_df.withColumn("items_arr", split($"items"," "))

    val minSupport = 0.2
    val minConfidence = 0.8
    val numPartitions = 10

    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)
    val fpg_model = fpg.run(rdd2)

//    val fpg_model_path = ""
//    fpg_model.save(sc, fpg_model_path)

    fpg_model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    fpg_model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
        s"${rule.consequent .mkString("[", ",", "]")},${rule.confidence}")
    }

    /*

    参考链接：
    How should I use the rules provided by FPGrowth in Scala?
    https://stackoverflow.com/questions/48580819/how-should-i-use-the-rules-provided-by-fpgrowth-in-scala
     */

    val exampleShoppingCart = Array("r", "z", "h", "k", "p")
    val proposals = fpg_model.generateAssociationRules(minConfidence).map{ rule =>
      val antecedent = rule.antecedent//.asInstanceOf[Seq[String]]
      val consequent = rule.consequent//.asInstanceOf[Seq[String]]
      if (antecedent.forall(exampleShoppingCart.contains)) {
        consequent.toSet
      } else {
        Set.empty[String]
      }
    }.reduce(_ ++ _)

    println("=================")
    proposals.foreach(println)
    println("=================")
    println("proposals size is: "+ proposals.size)

    val nonTrivialProposals = proposals.filterNot(exampleShoppingCart.contains)

    println(
      "Your shopping cart: " + exampleShoppingCart.mkString(",") +
        "; You might also be interested in: " + nonTrivialProposals
    )



    val rules = fpg_model.generateAssociationRules(minConfidence).map { rule =>
      val antecedent = rule.antecedent //.asInstanceOf[Seq[String]]
    val consequent = rule.consequent //.asInstanceOf[Seq[String]]
      (antecedent, consequent)
    }.collect()//.asInstanceOf[Array[(Seq[String], Seq[String])]]

    val brRules = sc.broadcast(rules)

    val test_arr = Array("r", "z", "h", "k", "p")

    val pred_test_arr = if (test_arr != null){
      brRules.value.flatMap(rule =>
        if (test_arr != null && rule._1.forall(arr => test_arr.contains(arr))){
          rule._2.filter(arr => !test_arr.contains(arr))
        } else {
          Seq.empty[String]
        }
      ).distinct
    } else {
      Seq.empty[String]
    }

    println("*****************")
    println(pred_test_arr)
    println("*****************")


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



    val fpg_model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/fpg_model"
//    fpg_model.save(sc,fpg_model_path)
    val fpg_model_reload = FPGrowthModel.load(sc,fpg_model_path)
    def predict(ipt:String, sep:String): String ={

      val ipt_arr = ipt.split(sep)
      val proposals = fpg_model_reload.generateAssociationRules(minConfidence).map{ rule =>
        val antecedent = rule.antecedent//.asInstanceOf[Seq[String]]
      val consequent = rule.consequent//.asInstanceOf[Seq[String]]
        if (antecedent.forall(ipt_arr.contains)) {
          consequent.toSet
        } else {
          Set.empty[String]
        }
      }.reduce(_ ++ _)

      val nonTrivialProposals = proposals.filterNot(ipt_arr.contains).mkString(sep)
      nonTrivialProposals

    }

    val pred_rule = udf((ipt:String) => predict(ipt," "))


    val train_pred = train_df.withColumn("pred", pred_rule($"items"))
    train_pred.show(truncate = false)

    val test_pred = test_df.withColumn("pred", pred_rule($"items"))
    test_pred.show(truncate = false)

    /*

    val rules = fpg_model.generateAssociationRules(minConfidence).map{
      rule => (rule.antecedent.toSeq, rule.consequent.toSeq)
    }.collect().asInstanceOf[Array[(Seq[String], Seq[String])]]

    val brRules = df1.sparkSession.sparkContext.broadcast(rules)

    val dt = df1.schema($(itemsCol)).dataType

    val predictUDF = udf((items: Seq[_]) => {
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
      }}, dt)
*/

    sc.stop()
    spark.stop()
  }

}
