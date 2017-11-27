package com.evayInfo.Inglory.TestCode

import org.ansj.splitWord.analysis.DicAnalysis
import org.ansj.util.FilterModifWord

/**
 * Created by sunlu on 17/11/27.
 */
object ansjTest3 {
  def main(args: Array[String]) {

    val s = "【热点-焦点】洛阳大数据产业园已入驻大数据产业项目300余家"
    val ansj = DicAnalysis.parse(s)
    println(ansj)
    val result = FilterModifWord.modifResult(ansj)
    println(result)
  }

}
