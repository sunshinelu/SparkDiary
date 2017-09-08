package com.evayInfo.Inglory.SparkDiary.files

/**
 * Created by sunlu on 17/8/17.
 */
object readText {

  /*

      //read log files
    val logsRDD = sc.textFile("/app-ylzx-logs").filter(null != _)
//    val logsRDD = sc.textFile("/personal/sunlu/ylzx_app").filter(null != _)

    val logsRDD2 = logsRDD.map(_.split("\t")).filter(_.length == 11).filter(_ (4).length > 2).map(line => (LogView(line(4), line(8), line(10))))

    //过滤REQUEST_URI列中包含search/getContentById.do的列，并提取PARAMS中的id
    val logsRDD3 = logsRDD2.filter(x => x.REQUEST_URI.contains("getContentById.do")).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "")
        Schema1(userID, urlString)
      }).filter(_.itemString.length >= 10)
   */

}
