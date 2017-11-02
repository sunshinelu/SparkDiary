package com.evayInfo.Inglory.ScalaDiary

/**
 * Created by sunlu on 17/11/2.
 */
object unzipDemo1 {

  def main(args: Array[String]) {
    val alphabet = List("A", "B", "C")
    val nums = List(1, 2)

    val zipped = alphabet zip nums // List(("A",1),("B",2))
    println(zipped)

    val zippedAll = alphabet.zipAll(nums, "*", -1) // List(("A",1),("B",2),("C",-1))
    println(zippedAll)

    val zippedIndex = alphabet.zipWithIndex // List(("A",0),("B",1),("C",3))
    println(zippedIndex)


    val (list1, list2) = zipped.unzip // list1 = List("A","B"), list2 = List(1,2)
    println(list1)
    println(list2)

    val (l1, l2, l3) = List((1, "one", '1'), (2, "two", '2'), (3, "three", '3')).unzip3
    println(l1)
    println(l2)
    println(l3)

  }
}
