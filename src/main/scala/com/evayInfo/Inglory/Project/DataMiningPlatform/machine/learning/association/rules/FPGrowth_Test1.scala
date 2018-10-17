package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/10/11.
 * 添加spark 2.3.2中spark-mllib依赖，使用ml中的FPGrowth方法构建。
 *         <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-mllib_${scala.binary.version}</artifactId>
            <version>2.3.2</version>
        </dependency>
 */
object FPGrowth_Test1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"FPGrowth_Test1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    /*
    val dataset = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2")
    ).toDF("items")
    */
    val dataset = spark.createDataset(Seq(
      "r z h k p",
      "z y x w v u t s",
      "s x o n r",
      "x z y m t s q e",
      "z",
      "x z y r q t p")
    ).toDF("items")

    dataset.show()

    val df1 = dataset.withColumn("items_arr", split($"items"," "))

    df1.show()
    df1.printSchema()

    val fpgrowth = new FPGrowth().setItemsCol("items_arr").setMinSupport(0.5).setMinConfidence(0.6)
    val model = fpgrowth.fit(df1)

    // Display frequent itemsets.
    println("Display frequent itemsets")
    model.freqItemsets.show()
    /*
+------------+----+
|       items|freq|
+------------+----+
|         [r]|   3|
|         [z]|   5|
|         [s]|   3|
|      [s, x]|   3|
|         [x]|   4|
|      [x, z]|   3|
|         [t]|   3|
|      [t, x]|   3|
|   [t, x, z]|   3|
|      [t, z]|   3|
|         [y]|   3|
|      [y, t]|   3|
|   [y, t, x]|   3|
|[y, t, x, z]|   3|
|   [y, t, z]|   3|
|      [y, x]|   3|
|   [y, x, z]|   3|
|      [y, z]|   3|
+------------+----+

     */

    // Display generated association rules.
    println("Display generated association rules.")
    model.associationRules.show(truncate=false)
    /*
+----------+----------+----------+
|antecedent|consequent|confidence|
+----------+----------+----------+
|    [y, t]|       [x]|       1.0|
|    [y, t]|       [z]|       1.0|
| [y, x, z]|       [t]|       1.0|
| [y, t, z]|       [x]|       1.0|
|       [y]|       [t]|       1.0|
|       [y]|       [x]|       1.0|
|       [y]|       [z]|       1.0|
|    [x, z]|       [t]|       1.0|
|    [x, z]|       [y]|       1.0|
|       [t]|       [x]|       1.0|
|       [t]|       [z]|       1.0|
|       [t]|       [y]|       1.0|
| [y, t, x]|       [z]|       1.0|
|       [x]|       [s]|      0.75|
|       [x]|       [z]|      0.75|
|       [x]|       [t]|      0.75|
|       [x]|       [y]|      0.75|
|    [y, z]|       [t]|       1.0|
|    [y, z]|       [x]|       1.0|
|    [t, x]|       [z]|       1.0|
+----------+----------+----------+

     */

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    println("consequents as prediction")
    model.transform(df1).show()
    /*
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


    val rdd1 = df1.select("items_arr").rdd.map{case Row(x:Seq[String]) => x}
    val rdd2 = rdd1.flatMap(x => x.sorted.combinations(2))
    rdd2.foreach(println)

    rdd2.toDF("col1").show()

    val df2 = rdd2.map{x => (x(0),x(1))}.toDF("col1","col2")
    df2.show()

    sc.stop()
    spark.stop()
  }

}
