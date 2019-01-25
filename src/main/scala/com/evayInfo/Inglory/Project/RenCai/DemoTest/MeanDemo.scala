package com.evayInfo.Inglory.Project.RenCai.DemoTest

import breeze.linalg.sum


object MeanDemo {

  def main(args: Array[String]): Unit = {

    def mean_func(x1:Double, x2:Double,x3:Double,x4:Double, x5:Double,x6:Double):Double={

      val arr1 = Array(x1,x2,x3,x4,x5,x6)
      val sum_value = arr1.sum
//      val sum_value = sum(x1,x2,x3,x4,x5,x6).toString.toDouble
      val mean_value = sum_value / arr1.length
      return mean_value
    }


    val t1 = mean_func(50.0,0.0,0.0,0.0,0.0,0.0)
    println(s"t1 is: $t1")
  }
}
