package com.evayInfo.Inglory.NLP.HanLP

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.io.IOUtil
import com.hankcs.hanlp.mining.word.WordInfo
import java.io.IOException
import java.util
import java.util.List


object DemoNewWordDiscover {

  def main(args: Array[String]): Unit = {

    val txt = "俞汝勤（1935－），分析化学家，中国科学院院士。湖南长沙人。毕业于长沙雅礼中学。1953年入苏联列宁格勒矿业学院化学系学习，1959年毕业。回国后在中科院长春应用化学研究所工作。1962年调湖南大学，历任化工系教授、湖南大学校长等职。1984年加入共产党，现任中国化学学会理事、《化学传感器》主编。专长于有机分析试剂和化学计量学研究，所主持的“氟离子选择性电极研究”和“有机试剂用于电化学及催化动力分析研究”，分别获1978年全国科学大会奖和1987年国家自然科学奖。著有《现代分析化学与信息理论基础》、《化学计量学导论》等。1991年当选为中国科学院化学部委员，1994年改称院士。"
    val wordInfoList = HanLP.extractWords(txt, 100)
    println(wordInfoList)
  }

}
