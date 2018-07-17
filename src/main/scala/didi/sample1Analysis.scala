package didi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/7/17.
 */
object sample1Analysis {
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

    val sample1_file_path = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/didi/sample1.csv"

    // 读取csv文件，含表头
    val colName_sample1 = Seq("week", "hour", "ckjds", "sjydds","wcds", "zsxsh")
    // "week", "hour", "ckjds", "sjydds",         "wcds",   "zsxsh"   分别对应
    //   周	    时刻	  乘客叫单数	 司机应答的单数	 完成的单数	  在线司机数
    val sample1_df = spark.read.option("header", true).option("delimiter", ",").
      csv(sample1_file_path).toDF(colName_sample1: _*)

    /*
    1	本周哪天的完成行程最高?完成了多少单
     */

    // 对week进行分组，对每组的wcds（完成的单数）进行求和
    val df1 = sample1_df.groupBy("week").agg(sum("wcds")).orderBy($"sum(wcds)".desc)
    df1.show()
/*
+----+---------+
|week|sum(wcds)|
+----+---------+
|  周五|  99188.0|
|  周四|  84604.0|
|  周三|  80257.0|
|  周二|  73522.0|
|  周六|  73089.0|
|  周日|  69406.0|
|  周一|  68841.0|
+----+---------+
本周周五的行程最高，周五完成的单数为99188

 */

    /*
    2	根据本周数据，连续12个小时中累计完成行程最多的时段是什么时候，完成了多少单？
     */

//    val list1 = List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24)
//    val list2 = list1.sorted.combinations(12)
//    list2.foreach(println)

    for (a <- 1 to 24){
      if(a + 11 <= 24)
        println(a to a + 11)
//      else ()
    }



    /*
    3	周五有多大比例的单子没有司机应答？
     */



    /*
    4	维持合理的需求／供给比例是高效运营的关键，你觉得有什么指标能体现供需状况。根据你的指标，找到一天中最供不应求的时刻作为高峰期。
     */


    /*
    5	应答率是客户体验的一个重要指标。在一定时间内（比如一小时）上线司机越多，应答率就越高吗？
     */



    sc.stop()
    spark.stop()

  }

}
