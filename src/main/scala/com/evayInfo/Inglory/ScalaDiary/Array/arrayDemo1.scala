package com.evayInfo.Inglory.ScalaDiary.Array

import scala.collection.mutable.ArrayBuffer

/**
 * Created by sunlu on 17/12/27.
 */
object arrayDemo1 {
  def main(args: Array[String]) {
    val maxIter: Array[Int] = Array[Int](5, 10, 20, 50, 100, 200)
    val accuracy: Array[Double] = Array[Double](0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    val b = ArrayBuffer[Double]()

    for (i <- 0 to maxIter.length -1 ){
      println(maxIter(i))
      accuracy(i) =i
      println(accuracy.toList)
      b += maxIter(i)
    }
    println(b.toList)
  }

}
