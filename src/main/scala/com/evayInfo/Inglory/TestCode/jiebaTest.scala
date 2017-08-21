package com.evayInfo.Inglory.TestCode

import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode

/**
 * Created by sunlu on 17/7/31.
 */
object jiebaTest {
  def main(args: Array[String]) {
    val s = "大数据hello工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作"
    val segmenter = new JiebaSegmenter()

    val seg = segmenter.process(s, SegMode.INDEX).toString()
    println(seg)

    val seg2 = segmenter.process(s, SegMode.SEARCH).toString()
    println(seg2)

    val seg3 = segmenter.process(s, SegMode.SEARCH).toArray()
    println(seg3)

    //    seg2.word.getTokenType

    val s2 = Seq("这是一个伸手不见五指的黑夜。我叫孙悟空，我爱北京，我爱Python和C++。", "工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作", "结果婚的和尚未结过婚的")
    for (sentence <- s2) {
      println(segmenter.process(sentence, SegMode.INDEX).toString())
    }

  }
}
