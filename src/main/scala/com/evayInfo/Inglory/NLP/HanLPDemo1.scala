package com.evayInfo.Inglory.NLP

import com.hankcs.hanlp.HanLP

/**
 * Created by sunlu on 17/9/1.
 * 使用HanLP分词参考连接：
 * https://github.com/hankcs/HanLP
 * https://github.com/hankcs/HanLP/tree/master/src/test/java/com/hankcs/demo
 * http://www.hankcs.com/nlp/hanlp.html
 */
object HanLPDemo1 {

  def main(args: Array[String]) {


    /*
    使用HanLP进行关键词提取
     */
    val content: String = "程序员(英文Programmer)是从事程序开发、维护的专业人员。一般将程序员分为程序设计人员和程序编码人员，但两者的界限并不非常清楚，特别是在中国。软件从业人员分为初级程序员、高级程序员、系统分析员和项目经理四大类。"
    val keywordList = HanLP.extractKeyword(content, 5).toArray().mkString(";")
    println(keywordList)


  }

}
