package com.evayInfo.Inglory.TestCode

/**
 * Created by sunlu on 17/9/20.
 */
object regTest {
  def main(args: Array[String]) {
//    val hotlabelString = "{manuallabel=政务, sType=index}"
    val hotlabelString =  "{manuallabel=, cuPage=1, xzqhname=中国}"
    //    val reg_hotlabel = """manuallabel=.+(,|})""".r
    //    val reg_hotlabel = """manuallabel=.+,|manuallabel=.+}""".r
    val reg_hotlabel =
      """manuallabel=([\u4e00-\u9fa5]|[a-zA-Z])+""".r
    val hotlabel = reg_hotlabel.findFirstIn(hotlabelString).toString.replace("Some(manuallabel=", "").replace(",)", "").replace("}", "").replace(")", "")

    println("hotlabel is: " + hotlabel)

    val searchString = "{cuPage=1, keyword=大数据}"
    val reg_search = """keyword=.+(,|})""".r
    val searchWords = reg_search.findFirstIn(searchString).toString.replace("Some(keyword=", "").replace(",)", "").replace("}", "").replace(")", "")
    println("searchWords is: " + searchWords)

  }
}
