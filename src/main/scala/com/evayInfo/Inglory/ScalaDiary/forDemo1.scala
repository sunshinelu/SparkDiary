package com.evayInfo.Inglory.ScalaDiary

/**
 * Created by sunlu on 17/9/7.
 */
object forDemo1 {

  def main(args: Array[String]) {

    // 打印1到10包括10
    for (i <- 1 to 10) {
      println("i is " + i)
    }


    // 打印1到9，不包含10
    for (i <- 1 until 10) {
      println("i is " + i)
    }

  }

}
