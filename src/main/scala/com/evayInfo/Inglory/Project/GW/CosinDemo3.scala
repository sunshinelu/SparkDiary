package com.evayInfo.Inglory.Project.GW

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{DenseVector => SDV, Vector => MLVector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * Created by sunlu on 18/8/14.
 * 使用spark中的dataframe计算余弦相似度
 */
object CosinDemo3 {
  // do not print log
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  /**
   * 求向量的模
   * @param vec
   * @return
   */
  def module(vec: Vector[Double]) = {
    math.sqrt(vec.map(math.pow(_, 2)).sum)
  }

  /**
   * 求两个向量的内积
   * @param v1
   * @param v2
   * @return
   */
  def innerProduct(v1: Vector[Double], v2: Vector[Double]) = {
    val listBuffer = ListBuffer[Double]()
    for (i <- 0 until v1.length; j <- 0 to v2.length; if i == j) {
      if (i == j) listBuffer.append(v1(i) * v2(j))
    }
    listBuffer.sum
  }
  /**
   * 求两个向量的余弦
   * @param v1
   * @param v2
   * @return
   */
  def cosvec(v1: Vector[Double], v2: Vector[Double]) = {
    val cos = innerProduct(v1, v2) / (module(v1) * module(v2))
    if (cos <= 1) cos else 1.0
  }

  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"CosinDemo3").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val df1 = spark.createDataFrame(Seq(
      (0, Array(1.0, 3.0, 3.0)),
      (1, Array(1.0, 2.0, 3.0))
    )).toDF("id", "vec")
    df1.printSchema()

    val t1 = Array(1.0, 3.0, 3.0)
    val arr1 = Vectors.dense(t1)
//    val df2 = df1.withColumn("arr", lit(arr1))
//    val df2 = df1.withColumn("arr", array(t1))
    val df2 = df1.withColumn("arr", split(lit("1.0, 3.0, 3.0"),","))
    df2.show()
    df2.printSchema()

//    def sqdistFunc(v1:MLVector, v2:MLVector):Double = {
//      Vectors.sqdist(v1, v2)
//    }

    val sqdisUDF = udf((v1:MLVector, v2:MLVector) => Vectors.sqdist(v1, v2))

    val df3 = df2.withColumn("sqdist", sqdisUDF($"vec",$"arr"))
    df3.show()

    /*


    val arr2 = Vector(1.0, 3.0, 3.0)
//    val cosin_udf = udf(() => cosvec(col("args"),col("args")))
//
//    val df2 = df.withColumn("coSim", udf(cosvec, FloatType)(col("myCol"), Array((lit(v) for v in arr])))

    val arr2 = Array(for(i <- arr2) {
      lit(i)
    })

    println(arr2)

//    println(lit(arr))
*/

    val df = Seq(
      ("i like blue and red"),
      ("you pink and blue")
    ).toDF("word1")

    val actualDF1 = df.withColumn(
      "colors",
      array(
        when(col("word1").contains("blue"), "blue"),
        when(col("word1").contains("red"), "red"),
        when(col("word1").contains("pink"), "pink"),
        when(col("word1").contains("cyan"), "cyan")
      )
    )
    val colors = Array("blue", "red", "pink", "cyan")

    val actualDF2 = df.withColumn(
      "colors",
      array(
        colors.map{ c: String =>
          when(col("word1").contains(c), c)
        }: _*
      )
    )
    actualDF2.show(truncate=false)



    sc.stop()
    spark.stop()
  }

}
