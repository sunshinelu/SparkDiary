package com.evayInfo.Inglory.TestCode

/**
 * Created by sunlu on 17/9/20.
 */
object regTest {
  def main(args: Array[String]) {
    val hotlabelString = "{manuallabel=政务, sType=index}"
    //    val reg_hotlabel = """manuallabel=.+(,|})""".r
    val reg_hotlabel =
      """manuallabel=.+,|manuallabel=.+}""".r
    val hotlabel = reg_hotlabel.findFirstIn(hotlabelString).toString.replace("Some(manuallabel=", "").replace(",)", "").replace("}", "").replace(")", "")

    println("hotlabel is: " + hotlabel)

    val searchString = "{cuPage=1, keyword=大数据}"
    val reg_search = """keyword=.+(,|})""".r
    val searchWords = reg_search.findFirstIn(searchString).toString.replace("Some(keyword=", "").replace(",)", "").replace("}", "").replace(")", "")
    println("searchWords is: " + searchWords)

  }
}
