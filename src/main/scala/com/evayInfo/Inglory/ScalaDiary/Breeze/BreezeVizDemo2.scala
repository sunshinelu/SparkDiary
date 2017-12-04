package com.evayInfo.Inglory.ScalaDiary.Breeze

import breeze.linalg._
import breeze.stats.hist
import org.scalatest.FunSuite

/**
 * Created by sunlu on 17/11/29.
 */
object BreezeVizDemo2 extends FunSuite {

  def main(args: Array[String]) {

    val testDV = DenseVector(0.0, 0.1, 2.8, 2.9, 5)
    val testWeights = DenseVector(0.5, 0.5, 1.0, 3.0, 7.0)


      val result = hist(testDV, 3)
//     result.hist == DenseVector(2.0,2.0,1.0)
//     result.binEdges == DenseVector(0.0, 5.0/3.0, 2*5.0/3.0, 5.0)



  }

}
