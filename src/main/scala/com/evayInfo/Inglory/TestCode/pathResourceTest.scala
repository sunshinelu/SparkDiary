package com.evayInfo.Inglory.TestCode

import org.datavec.api.split.FileSplit
import org.datavec.api.util.ClassPathResource

/**
 * Created by sunlu on 17/11/23.
 */
object pathResourceTest {
  def main(args: Array[String]) {

    val t0 = new ClassPathResource("core-site.xml")
    println(t0)

    val t1 = new ClassPathResource("core-site.xml").getFile()
    println(t1)
    ///Users/sunlu/Documents/workspace/IDEA/SparkDiary/target/classes/core-site.xml

    val t2 = new FileSplit(new ClassPathResource("core-site.xml").getFile())
    println(t2)
  }
}
