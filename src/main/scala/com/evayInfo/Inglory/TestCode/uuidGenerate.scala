package com.evayInfo.Inglory.TestCode

import java.util.UUID

/**
 * Created by sunlu on 17/7/27.
 * 使用rt.jar生成UUID
 */
object uuidGenerate {
  def main(args: Array[String]) {
    val s1 = UUID.randomUUID().toString().toLowerCase()
    println(s1)
  }
}
