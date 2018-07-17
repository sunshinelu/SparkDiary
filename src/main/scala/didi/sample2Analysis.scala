package didi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/7/17.
 */
object sample2Analysis {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("sample1Analysis").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val sample2_file_path = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/didi/sample2.csv"

    // 读取csv文件，含表头
    val colName_sample2 = Seq("week", "sjbh", "pjxj", "zxsc","qddjcs", "cgqds","wcdds", "ddsjzgls","cfsr")
    // week", "sjbh", "pjxj", "zxsc","qddjcs",    "cgqds","wcdds",    "ddsjzgls",   "cfsr"  分别对应
    //   日期	司机编号	平均星级	在线时长	抢单点击次数	成功抢单数	完成订单数	订单实际总公里数  	车费收入
    val sample2_df = spark.read.option("header", true).option("delimiter", ",").
      csv(sample2_file_path).toDF(colName_sample2: _*)

    /*
    6	这批司机当中，效率最高的司机是哪位？效率最低的呢？你觉得这些数据反映了什么问题。
     */


    /*
    7	平均每单行程多少公里，平均每单收入多少钱？
     */


    /*
    8	根据提供的数据，一个司机平均每小时能完成几单，平均每小时收入是多少钱？
     */

    /*
    9	唐师傅的车油耗是百公里9L，当前油价是6.6元／升，车价20万，假设车10年折旧，每年各项杂费8000元。
    唐师傅希望每周通过滴滴平台实现净收入2000元，那么他平均需要工作多少小时？（可参考之前题目的答案）
     */


    sc.stop()
    spark.stop()
  }

}
