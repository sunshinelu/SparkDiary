package com.evayInfo.Inglory.Project.sentiment

/**
 * Created by sunlu on 17/7/18.
 * 参考链接：
 * https://www.zhihu.com/question/19816153
 * http://blog.csdn.net/wqs1028/article/details/50388393
 */
object regTest {
  def main(args: Array[String]) {
    val s = "今晚派个有点儿帅的你何陪你。@快乐大本营 @快乐大本营2 #快本二十正青春##变形金刚5#"

    /*
    提取话题
     */
    val reg1 = "#\\S*#".r
    val p1 = reg1.findAllMatchIn(s).mkString(";")
    println(p1)
    //输出：#快本二十正青春##变形金刚5#

    //this one is the best!
    val reg4 = "#[^#]+#".r
    val p4 = reg4.findAllMatchIn(s).mkString(";")
    println(p4)
    /*
    提取@用户
     */

    val reg2 = "@\\S*\\s".r
    val p2 = reg2.findAllMatchIn(s).mkString(";")
    println(p2)

    //this one is the better!
    val reg3 = "@[^,，：:\\s@]+".r
    val p3 = reg3.findAllMatchIn(s).mkString(";")
    println(p3)

    //this one is the best!
    val reg5 = "@[\\u4e00-\\u9fa5a-zA-Z0-9_-]{4,30}".r
    val p5 = reg5.findAllMatchIn(s).mkString(";")
    println(p5)
    println("替换提取@用户：" + reg5.replaceAllIn(s, ""))

    /*
提取转发用户
 */

    val ss = "对得真妙@何炅 //@何炅:前路，也阳光//@李维嘉:回望，太美好"
    val ss2 = "这属于侵略中国，符合触发武力统一的条件。//@你好台湾网：【美参院军事委员会：允许军舰停靠台湾】据台媒，美参议院军事委员会在两党的支持下，同意美国海军军舰可定期停靠高雄等台湾港口，并将台湾纳入美国一项年度国防政策措施之中。这项决议一旦获得国会批准，那将意味着美国近40年来对台政策的]"

    val reg6 = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+".r
    val p6 = reg6.findAllMatchIn(ss2).mkString(";")
    println("p6 is: " + p6)
    //提取结果为：//@何炅;//@李维嘉

    /*
       提取转发全部内容
        */

    //    val ss2 = "这属于侵略中国，符合触发武力统一的条件。//@你好台湾网：【美参院军事委员会：允许军舰停靠台湾】据台媒，美参议院军事委员会在两党的支持下，同意美国海军军舰可定期停靠高雄等台湾港口，并将台湾纳入美国一项年度国防政策措施之中。这项决议一旦获得国会批准，那将意味着美国近40年来对台政策的]"
    //    val reg7 = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+:[\\u4e00-\\u9fa5a-zA-Z0-9_：【】,.?:;'\"!，。！“”；？]+".r
    val reg7 = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+[\\u4e00-\\u9fa5a-zA-Z0-9_：【】,.?:;'\"!，。！“”；？]+".r
    val p7 = reg7.findAllMatchIn(ss2).mkString(";")
    println("p7 is: " + p7)
    println(reg7.replaceAllIn(ss2, ""))
    /*
    提取正文
     */
    val reg8 = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+[\\u4e00-\\u9fa5a-zA-Z0-9_：【】,.?:;'\"!，。！“”；？]+|@[^,，：:\\s@]+|#[^#]+#".r
    val p8 = reg8.replaceAllIn(ss2, "")
    println("p8 is: " + p8)

    /*
    表情符号的替换
     */

    val sss = "#呵呵呵#[偷笑] http://test.com/blah_blah"
    val reg9 = "\\[[0-9a-zA-Z\\u4e00-\\u9fa5]+\\]".r
    val p9 = reg9.replaceAllIn(sss, "")
    println("p9 is: " + p9)

    /*
    提取URL
     */

    val reg10 = "\\b(([\\w-]+://?|www[.])[^\\s()<>]+(?:\\([\\w\\d]+\\)|([^[:punct:]\\s]|/)))".r
    val p10 = reg10.findAllMatchIn(sss).mkString(";")
    println("p10 is: " + p10)

    /*
    提取回复信息
     */

    val ss3 = "回复@你好台湾网:其实特别简单"
    val reg11 = "回复@[^,，：:\\s@]+[:：]".r
    val p11 = reg11.findAllMatchIn(ss3).mkString(";")
    println("提取回复信息p11 is: " + p11)


  }
}
