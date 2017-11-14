package com.evayInfo.Inglory.ScalaDiary

import java.util.Random

/**
 * Created by sunlu on 17/11/13.
 */
object randomDemo1 {
  def main(args: Array[String]) {
    val rng: Random = new Random(12345)
    println(rng.toString)
    rng.formatted("a").foreach(println)
  }
}
