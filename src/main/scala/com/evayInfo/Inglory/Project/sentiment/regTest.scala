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
    val reg6 = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+".r
    val p6 = reg6.findAllMatchIn(ss).mkString(";")
    println("p6 is: " + p6)
    //提取结果为：//@何炅;//@李维嘉

    /*
       提取转发全部内容
        */

    val reg7 = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+:[\\u4e00-\\u9fa5a-zA-Z0-9_,.?:;'\"!，。！“”：；？]+".r
    val p7 = reg7.findAllMatchIn(ss).mkString(";")
    println("p7 is: " + p7)
    println(reg7.replaceAllIn(ss, ""))
    /*
    提取正文
     */
    val reg8 = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+:[\\u4e00-\\u9fa5a-zA-Z0-9_,.?:;'\"!，。！“”：；？]+|@[^,，：:\\s@]+|#[^#]+#".r
    val p8 = reg8.replaceAllIn(s, "")
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



  }
}
