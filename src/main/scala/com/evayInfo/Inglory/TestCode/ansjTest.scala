package com.evayInfo.Inglory.TestCode

import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.MyStaticValue

/**
 * Created by sunlu on 17/8/21.
 */
object ansjTest {
  def main(args: Array[String]) {

    //在用词典未加载前可以通过,代码方式方式来加载
    MyStaticValue.userLibrary = "library/userDefine.dic"
    val s = "大数据hello工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作"
    val seg1 = ToAnalysis.parse(s)
    println(seg1)
    /*
    [大数据/userDefine, hello/en, 工/n, 信/n, 处/n, 女/b, 干事/n, 每月/r, 经过/p, 下属/v, 科室/n, 都/d, 要/v, 亲口/d, 交代/v, 24口/m, 交换机/n, 等/u, 技术性/n, 器件/n, 的/uj, 安装/v, 工作/vn]

     */


  }
}
