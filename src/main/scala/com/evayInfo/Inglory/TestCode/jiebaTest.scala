package com.evayInfo.Inglory.TestCode

import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode

/**
 * Created by sunlu on 17/7/31.
 */
object jiebaTest {
  def main(args: Array[String]) {
    val s = "工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作"
    val segmenter = new JiebaSegmenter()

    val seg = segmenter.process(s, SegMode.INDEX).toString()
    println(seg)

    val seg2 = segmenter.process(s, SegMode.SEARCH).toString()
    println(seg2)

  }
}
