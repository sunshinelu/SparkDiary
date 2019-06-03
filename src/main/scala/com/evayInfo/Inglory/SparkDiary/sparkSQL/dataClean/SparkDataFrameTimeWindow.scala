package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

/**
  * @Author: sunlu
  * @Date: 2019-06-03 16:40
  * @Version 1.0
  *
  *  参考链接：
  *  在 Spark DataFrame 中使用Time Window
  *  https://blog.csdn.net/wangpei1949/article/details/83855223
  *  https://github.com/turtlebin/spark/blob/ad841954b7d23fb345721d439e27ecda7024a309/SparkCommerce/src/main/scala/xhb/SparkCommerce/SparkSqlWindow.scala
  *  https://github.com/tarak-mpc/cppentry.github.io/blob/4ef0953ae37dcbe7f1a99263d65ef0477b0599d9/_posts/2018-11-22-%E5%9C%A8%20Spark%20DataFrame%20%E4%B8%AD%E4%BD%BF%E7%94%A8Time%20Window.markdown
  *
  */
object SparkDataFrameTimeWindow {
  def main(args: Array[String]): Unit = {
    //设置日志等级
    Logger.getLogger("org").setLevel(Level.WARN)

    //spark环境
    val spark = SparkSession.builder().master("local[3]").
      appName(this.getClass.getSimpleName.replace("$","")).getOrCreate()
    import spark.implicits._

    //读取时序数据
    val data = spark.read.option("header","true").option("inferSchema","true").csv("data/cpu_memory_disk_monitor.csv")
    //data.printSchema()

    /** 1) Tumbling window */
    /** 计算: 每10分钟的平均CPU、平均内存、平均磁盘，并保留两位小数 */
    data
      .filter(functions.year($"eventTime").between(2017,2018))
      .groupBy(functions.window($"eventTime","10 minute")) //Time Window
      .agg(functions.round(functions.avg($"cpu"),2).as("avgCpu"),functions.round(functions.avg($"memory"),2).as("avgMemory"),functions.round(functions.avg($"disk"),2).as("avgDisk"))
      .sort($"window.start").select($"window.start",$"window.end",$"avgCpu",$"avgMemory",$"avgDisk")
      .limit(5)
      .show(false)
    /*
+---------------------+---------------------+------+---------+-------+
|start                |end                  |avgCpu|avgMemory|avgDisk|
+---------------------+---------------------+------+---------+-------+
|2017-12-31 23:20:00.0|2017-12-31 23:30:00.0|3.6   |28.35    |58.0   |
|2017-12-31 23:30:00.0|2017-12-31 23:40:00.0|3.39  |28.69    |58.0   |
|2017-12-31 23:40:00.0|2017-12-31 23:50:00.0|3.44  |28.78    |59.0   |
|2017-12-31 23:50:00.0|2018-01-01 00:00:00.0|3.1   |29.02    |59.0   |
|2018-01-01 00:00:00.0|2018-01-01 00:10:00.0|10.49 |38.49    |59.5   |
+---------------------+---------------------+------+---------+-------+
     */

    /** 2) Slide window */
    /** 计算：从第3分钟开始，每5分钟计算最近10分钟内的平均CPU、平均内存、平均磁盘，并保留两位小数 */
    data
      .filter(functions.year($"eventTime").between(2017,2018))
      .groupBy(functions.window($"eventTime","10 minute","5 minute","3 minute")) //Time Window
      .agg(functions.round(functions.avg($"cpu"),2).as("avgCpu"),functions.round(functions.avg($"memory"),2).as("avgMemory"),functions.round(functions.avg($"disk"),2).as("avgDisk"))
      .sort($"window.start").select($"window.start",$"window.end",$"avgCpu",$"avgMemory",$"avgDisk")
      .limit(5)
      .show(false)
    /*
+---------------------+---------------------+------+---------+-------+
|start                |end                  |avgCpu|avgMemory|avgDisk|
+---------------------+---------------------+------+---------+-------+
|2017-12-31 23:13:00.0|2017-12-31 23:23:00.0|2.87  |28.23    |58.0   |
|2017-12-31 23:18:00.0|2017-12-31 23:28:00.0|3.6   |28.35    |58.0   |
|2017-12-31 23:23:00.0|2017-12-31 23:33:00.0|3.74  |28.6     |58.0   |
|2017-12-31 23:28:00.0|2017-12-31 23:38:00.0|3.39  |28.69    |58.0   |
|2017-12-31 23:33:00.0|2017-12-31 23:43:00.0|3.44  |28.67    |58.5   |
+---------------------+---------------------+------+---------+-------+

     */

    spark.stop()
  }


}
