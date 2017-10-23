package com.evayInfo.Inglory.SparkDiary.mllib.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/10/23.
 * 参考链接
 * Spark Distributed matrix 分布式矩阵：http://www.cnblogs.com/wwxbi/p/6815685.html
 */
object DistributedMatrixDemo1 {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"DistributedMatrixDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    RowMatrix行矩阵
     */
    val df1 = Seq((1.0, 2.0, 3.0), (1.1, 2.1, 3.1), (1.2, 2.2, 3.2)).toDF("c1", "c2", "c3")
    df1.show(false)

    // DataFrame转换成RDD[Vector]
    val rowsVector = df1.rdd.map {
      x => Vectors.dense(
        x(0).toString().toDouble,
        x(1).toString().toDouble,
        x(2).toString().toDouble)
    }

    val rowsVector2 = df1.rdd.map {
      case Row(x1: Double, x2: Double, x3: Double) => Vectors.dense(x1, x2, x3)
    }

    // Create a RowMatrix from an RDD[Vector].
    val mat1: RowMatrix = new RowMatrix(rowsVector)
    // Get its size.
    val m = mat1.numRows()
    val n = mat1.numCols()


    // 将RowMatrix转换成DataFrame
    val resDF = mat1.rows.map { x => (x(0).toDouble, x(1).toDouble, x(2).toDouble) }.toDF("c1", "c2", "c3")
    resDF.show
    mat1.rows.collect().take(10)

    /*
    CoordinateMatrix坐标矩阵
     */
    val df = Seq(
      (0, 0, 1.1), (0, 1, 1.2), (0, 2, 1.3),
      (1, 0, 2.1), (1, 1, 2.2), (1, 2, 2.3),
      (2, 0, 3.1), (2, 1, 3.2), (2, 2, 3.3),
      (3, 0, 4.1), (3, 1, 4.2), (3, 2, 4.3)).toDF("row", "col", "value")
    df.show

    // 生成入口矩阵
    val entr = df.rdd.map { x =>
      val a = x(0).toString().toLong
      val b = x(1).toString().toLong
      val c = x(2).toString().toDouble
      MatrixEntry(a, b, c)
    }

    // 生成坐标矩阵
    val mat: CoordinateMatrix = new CoordinateMatrix(entr)
    mat.numRows()

    mat.numCols()

    mat.entries.collect().take(10)

    // 坐标矩阵转成，带行索引的DataFrame，行索引为行坐标
    val t = mat.toIndexedRowMatrix().rows.map { x =>
      val v = x.vector
      (x.index, v(0).toDouble, v(1).toDouble, v(2).toDouble)
    }

    t.toDF().show

    // 坐标矩阵转成DataFrame
    val t1 = mat.toRowMatrix().rows.map { x =>
      (x(0).toDouble, x(1).toDouble, x(2).toDouble)
    }

    t1.toDF().show

    sc.stop()
    spark.stop()
  }

}
