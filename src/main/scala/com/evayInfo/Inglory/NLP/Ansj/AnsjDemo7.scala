package com.evayInfo.Inglory.NLP.Ansj

import org.ansj.splitWord.analysis.ToAnalysis

/**
 * Created by sunlu on 18/7/25.
 */
object AnsjDemo7 {

  def main(args: Array[String]) {

    /*
解决分词时文档中有 “/”的报错的问题
参考资料：
ansj分词教程
https://blog.csdn.net/a360616218/article/details/75268959
 */

    val str = "/大数据时代当城市数据和社会关系被可视化，每个人都可能是福尔摩斯"
    val wordseg = ToAnalysis.parse(str)
    var result = ""
    for (i <- 0 to wordseg.size()-1){
      result = result + " " +  wordseg.get(i).getName()
    }
    println(result)

  }
}
