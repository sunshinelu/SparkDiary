package com.evayInfo.Inglory.Project.GW

import scala.collection.mutable.ListBuffer
import scala.collection.mutable

/**
 * Created by sunlu on 18/8/7.
 *
 * scala 简单实现余弦相似度
 * https://blog.csdn.net/k_wzzc/article/details/81415383
 */
object CosinDemo1 {
  def main(args: Array[String]) {

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
    /**
     * 余弦相似度
     * @param str1
     * @param str2
     * @return
     */
    def textCosine(str1: String, str2: String) = {
      val set = mutable.Set[Char]()
      // 不进行分词
      str1.foreach(set += _)
      str2.foreach(set += _)
      val ints1: Vector[Double] = set.toList.sorted.map(ch => {
        str1.count(s => s == ch).toDouble
      }).toVector
      val ints2: Vector[Double] = set.toList.sorted.map(ch => {
        str2.count(s => s == ch).toDouble
      }).toVector
      cosvec(ints1, ints2)
    }

    val str1 = "这句话和下面那句很像"
    val str2 = "这句话和下面那句很像"//"我就是下面那句话"
    val d = textCosine(str1, str2)
    println("相似度：" + d)


  }

}
