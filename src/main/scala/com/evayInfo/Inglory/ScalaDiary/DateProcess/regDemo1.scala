package com.evayInfo.Inglory.ScalaDiary.DateProcess

/**
 * Created by sunlu on 18/1/22.
 *
 * 使用正则表达式提取字符串中 中括号里面的内容和删除中括号里面的内容
 *
jdbc_url=jdbc:mysql://192.168.37.104:33333/IngloryBDP?allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull
jdbc_username=root
jdbc_password=root
 *
 */
object regDemo1 {

  def main(args: Array[String]) {
    val s = "【西沙中建岛守备部队成立40周年，这组珍贵照片令人泪奔】对一场战斗最好的纪念是什么？1978年1月19日，西沙海战第4个纪念日。海军中建岛守备部队正式成立。18烈士的英雄血脉，以这样一种方式流传于白沙滩的军旗之上，护佑着祖宗海，也感召着一代代中建岛官兵。（当代海军）"

    // 提取中括号中的内容
    val reg1 = "【.*】".r
    val reg1Result = reg1.findFirstIn(s).get.replace("【", "").replace("】", "")
    println(reg1Result)

    // 将中括号中的内容替换成空
    val reg2 = "【.*】".r
    val reg2Result = reg2.replaceFirstIn(s,"")
    println(reg2Result)

  }

}
