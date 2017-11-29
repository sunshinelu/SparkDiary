package com.evayInfo.Inglory.ScalaDiary.Breeze

import breeze.linalg._
import breeze.numerics._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by sunlu on 17/8/23.
  * 参考链接：
  * http://www.itwendao.com/article/detail/400511.html
  */
object BreezeDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"BreezeDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    /*
    /*
    Breeze 创建函数
     */
    //创建0矩阵和向量
    val m1 = DenseMatrix.zeros[Double](2, 3)
    val v1 = DenseVector.zeros[Double](3)
    //创建元素都是1的向量
    val v2 = DenseVector.ones[Double](3)
    //创建指定元素的向量
    val v3 = DenseVector.fill(3)(5.0)
    //根据范围创建向量参数（start，end，step）
    val v4 = DenseVector.range(1, 10, 2)
    //创建对角线为1的矩阵
    val m2 = DenseMatrix.eye[Double](3)
    //创建指定对角线元素的矩阵
    val v6 = diag(DenseVector(1.0, 2.0, 3.0))
    //根据向量创建矩阵，每个数组就是一行
    val m3 = DenseMatrix((1.0, 2.0), (3.0, 4.0))
    //根据元素创建向量
    val v8 = DenseVector(1, 2, 3, 4)
    //val v9 = v8.t
    //转置
    val v9 = DenseVector(1, 2, 3, 4).t
    //根据下标创建向量和矩阵
    val v10 = DenseVector.tabulate(3) { i => 2 * i }
    val m4 = DenseMatrix.tabulate(3, 2) { case (i, j) => i + j }
    //根据数组创建向量和矩阵
    val v11 = new DenseVector(Array(1, 2, 3, 4))
    val m5 = new DenseMatrix(2, 3, Array(11, 12, 12, 21, 21, 11))
    //创建一个随机向量和矩阵
    val v12 = DenseVector.rand(4)
    val m6 = DenseMatrix.rand(2, 3)
    /*
    Breeze 元素访问
     */
    //元素访问
    val a = DenseVector(1, 2, 3, 4, 5, 6, 7, 8, 9)
    //访问指定的元素
    a(0)
    //访问子元素，返回还是一个向量
    a(1 to 4)
    //指定起始和终止位置，和补偿
    a(5 to 1 by -1)
    //-1 代表最后的元素
    a(1 to -1)
    //访问最后元素
    a(-1)

    val m = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
    //访问指定的元素
    m(0, 1)
    //访问某列的元素，返回一个向量
    m(::, 1)
    //访问某一行
    m(1, ::)

    //元素操作
    val m_1 = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
    //变成3行2列的矩阵
    m_1.reshape(3, 2)
    //生成一个向量
    m_1.toDenseVector

    val m_3 = DenseMatrix((1, 2, 3), (4, 5, 6), (7, 8, 9))
    //取上三角和下三角
    lowerTriangular(m_3)
    upperTriangular(m_3)
    //copy生成一个新的矩阵
    m_3.copy
    //对角线生成一个向量
    diag(m_3)
    //改变矩阵里面的元素
    m_3(::, 2) := 5
    m_3
    m_3(1 to 2, 1 to 2) := 5
    m_3

    //改变向量里面的元素
    val a_1 = DenseVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    a_1(1 to 4) := 5
    a_1
    a_1(1 to 4) := DenseVector(1, 2, 3, 4)
    a_1

    //矩阵的连接和向量的连接
    val a1 = DenseMatrix((1, 2, 3), (4, 5, 6))
    val a2 = DenseMatrix((1, 1, 1), (2, 2, 2))
    //竖直的连接
    DenseMatrix.vertcat(a1, a2)
    //水平连接
    DenseMatrix.horzcat(a1, a2)
    val b1 = DenseVector(1, 2, 3, 4)
    val b2 = DenseVector(1, 1, 1, 1)
    //水平连接
    DenseVector.vertcat(b1, b2)
    //两列的形式连接
    DenseVector.horzcat(b1, b2)
    /*
    Breeze 数值计算函数
     */
    val a_3 = DenseMatrix((1, 2, 3), (4, 5, 6))
    val b_3 = DenseMatrix((1, 1, 1), (2, 2, 2))
    //对应元素相操作
    a_3 + b_3
    a_3 :* b_3
    a_3 :/ b_3
    a_3 :< b_3
    a_3 :== b_3
    a_3 :+= 1
    a_3 :*= 2
    max(a_3)
    //最大值位置的索引
    argmax(a_3)
    //内积
    DenseVector(1, 2, 3, 4) dot DenseVector(1, 1, 1, 1)

    /*
    Breeze 求和函数
     */
    val a_4 = DenseMatrix((1, 2, 3), (4, 5, 6), (7, 8, 9))
    sum(a_4)
    //每一列进行求和 12 15 18
    sum(a_4, Axis._0)
    //每一行进行求和 DenseVector(6, 15,24)
    sum(a_4, Axis._1)
    //对角线求和
    trace(a_4)
    //把前面的元素相加 DenseVector(1, 3, 6, 10)
    accumulate(DenseVector(1, 2, 3, 4))

    /*
    Breeze 布尔函数
     */
    val a_5 = DenseVector(true, false, true)
    val b_5 = DenseVector(false, true, true)
    a_5 :& b_5
    a_5 :| b_5
    !a_5
    val a_5_2 = DenseVector(1, 0, -2)
    //任意一个元素为0即为true
    any(a_5_2)
    //所有元素都为0则为true
    all(a_5_2)
    /*
    Breeze 线性代数函数
     */
    val a_6 = DenseMatrix((1, 2, 3), (4, 5, 6), (7, 8, 9))
    val b_6 = DenseMatrix((1, 1, 1), (1, 1, 1), (1, 1, 1))
    a_6 \ b_6
    //转置
    a_6.t
    //特征值
    det(a_6)
    //逆
    inv(a_6)
    //矩阵分解（有问题）
    val svd.SVD(u, s, v) = svd(DenseMatrix(1.1, 2.0), (2.0, 3.0))
    a_6.rows
    a_6.cols

    /*
    Breeze 取整函数
     */

    val a_7 = DenseVector(1.2, 0.6, -2.3)
    //四舍五入
    round(a_7)
    //往前进位
    ceil(a_7)
    //都舍去
    floor(a_7)
    //正的变为1.0 负的变为-1 0还是0
    signum(a_7)
    //绝对值
    abs(a_7)

*/
    sc.stop()
    spark.stop()
  }
}
