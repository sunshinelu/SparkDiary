package com.evayInfo.Inglory.Project.RenCai

import java.text.SimpleDateFormat
import java.util.Properties

import breeze.linalg.{max, min, sum}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{bround, col, current_timestamp, date_format, lit, row_number, udf, max => f_max, min => f_min,sum => f_sum}
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel

/*


SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for relation_new
-- ----------------------------
DROP TABLE IF EXISTS `relation_new`;
CREATE TABLE `relation_new` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '序号',
  `source_id` varchar(255) NOT NULL COMMENT '人才ID',
  `source_name` varchar(50) NOT NULL COMMENT '人才名称',
  `target_id` varchar(36) NOT NULL COMMENT '相关联人才ID',
  `target_name` varchar(50) NOT NULL COMMENT '相关联人才名称',
  `relation` varchar(50) NOT NULL COMMENT '人才关系:同学，校友，同事，下属，领导，熟悉程度，领域相关',
  `relation_object` varchar(255) DEFAULT NULL COMMENT '关系对象，即学校或单位',
  `weight` double(6,3) NOT NULL COMMENT '权重 0.0~100.0',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for relation_shuxi
-- ----------------------------
DROP TABLE IF EXISTS `relation_shuxi`;
CREATE TABLE `relation_shuxi` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '序号',
  `source_id` varchar(255) NOT NULL COMMENT '人才ID',
  `source_name` varchar(50) NOT NULL COMMENT '人才名称',
  `target_id` varchar(36) NOT NULL COMMENT '相关联人才ID',
  `target_name` varchar(50) NOT NULL COMMENT '相关联人才名称',
  `relation` varchar(50) NOT NULL COMMENT '人才关系:同学，校友，同事，下属，领导，熟悉程度，领域相关',
  `relation_object` varchar(255) DEFAULT NULL COMMENT '关系对象，即学校或单位',
  `weight` double(6,3) NOT NULL COMMENT '权重 0.0~100.0',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


spark-shell --master yarn --num-executors 8 --executor-cores  8 --executor-memory 8g

 */
object FamiliarityRelation {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]): Unit = {

    // 获取当前时间
//    val d0 = new java.util.Date()
//    println("任务启动时间 " + d0.getTime)

    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"FamiliarityRelation").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/talent"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    /*
val url1 = "jdbc:mysql://10.20.7.156:3306/talent?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
val prop1 = new Properties()
prop1.setProperty("user", "root")
prop1.setProperty("password", "rcDsj_56")
 */
    val to_lower_udf = udf((x: String) => x.toLowerCase())

    def time_diff(s: String, e: String): Long = {
      //定义时间格式
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val s_L = dateFormat.parse(s).getTime
      val e_L = dateFormat.parse(e).getTime
      val result = (e_L - s_L) / (1000 * 3600 * 24) + 1
      return result
    }

    val time_diff_udf = udf((s: String, e: String) => time_diff(s, e))

    /*
    1. 专业技术职务表

     */


    val profession_ds1 = spark.read.jdbc(url1, "profession_posts", prop1)

    val profession_ds2 = profession_ds1.select("talent_id", "technical_title", "company", "engage_date").
      na.drop(Array("company")).filter($"company" =!= "无").
      withColumn("talent_id", to_lower_udf($"talent_id")).
      withColumn("engage_date", $"engage_date".cast("string")).
      na.fill(Map("technical_title" -> "无", "engage_date" -> "无")).
      dropDuplicates()

    val col_profession_temp1 = Seq("id_1", "technical_title_1", "company", "t1")
    val profession_temp1 = profession_ds2.toDF(col_profession_temp1: _*)
    val col_profession_temp2 = Seq("id_2", "technical_title_2", "company", "t2")
    val profession_temp2 = profession_ds2.toDF(col_profession_temp2: _*)

    val profession_ds3 = profession_temp1.join(profession_temp2, Seq("company"), "outer").
      filter($"id_1" =!= $"id_2")

    /*
聘任时间相差比较近就默认为两个人比较熟悉，比如A入职为2017.06.01，B入职为2018.01.01，入职时间差为半年，1-0.5/3 就作为最终结果
     */
    def profession_degree_date(t1: String, t2: String): Double = {
      val result = if (t1 == "无" | t2 == "无") {
        0.0
      } else {
        //定义时间格式
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val t1_L = dateFormat.parse(t1).getTime
        val t2_L = dateFormat.parse(t2).getTime

        val timeDiffer = (t1_L - t2_L) / (1000 * 3600 * 24) + 1
        val timeDiffer_Year = Math.abs(timeDiffer / (3 * 365.0))
        val weight = if (timeDiffer_Year >= 1) {
          1.0
        } else {
          timeDiffer_Year
        }
        ((1 - weight) * 100).toDouble
      }
      return result
    }
    val profession_degree_date_udf = udf((t1: String, t2: String) => profession_degree_date(t1, t2))

    def profession_degree_title(t1: String, t2: String): Double = {
      val result = if (t1 == t2 & t1 != "无" & t2 != "无") {
        1.0
      } else {
        0.0
      }
      return result * 100
    }
    val profession_degree_title_udf = udf((title_1: String, title_2: String) => profession_degree_title(title_1, title_2))

    val profession_ds4 = profession_ds3.withColumn("degree_date", profession_degree_date_udf($"t1", $"t2")).
      withColumn("degree_title", profession_degree_title_udf($"technical_title_1", $"technical_title_2")).
      withColumn("degree", bround($"degree_date" * 0.5 + $"degree_title" * 0.5, 3))

    val profession_col = Seq("id_1","id_2","degree_profession")
    val profession_ds5 = profession_ds4.select("id_1", "id_2", "degree").toDF(profession_col:_*)
    val profession_ds6 = profession_ds4.select("id_2","id_1","degree").toDF(profession_col:_*)
    val profession_ds7 = profession_ds5.union(profession_ds6).
      groupBy("id_1","id_2").agg(f_max($"degree_profession")).
      drop("degree_profession").toDF(profession_col:_*)


    // 新增一列递增列
//    val w1 = Window.partitionBy("id_1", "id_2").orderBy(col("degree_profession").desc)
//    val test1 = profession_ds7.withColumn("col3", row_number().over(w1))//.filter($"col3" >= 2)
//    test1.show(truncate = false)
//
//
//    profession_ds4.filter($"id_1" === "00281988-b02b-4954-85e3-2bada3032fa2" &&
//      $"id_2" === "2f700da9-178f-45c5-924d-ddc3476ca344").show(truncate = false)
//    profession_ds5.filter($"id_1" === "00281988-b02b-4954-85e3-2bada3032fa2" &&
//      $"id_2" === "2f700da9-178f-45c5-924d-ddc3476ca344").show(truncate = false)
//    profession_ds6.filter($"id_1" === "00281988-b02b-4954-85e3-2bada3032fa2" &&
//      $"id_2" === "2f700da9-178f-45c5-924d-ddc3476ca344").show(truncate = false)


//    println(profession_ds7.count())
//    println(profession_ds7.dropDuplicates().count())
//    println(profession_ds7.select("id_1","id_2").dropDuplicates().count())
//    println(profession_ds7.groupBy("id_1","id_2").agg(f_sum($"degree_profession")).count())
//
    /*
    2. 主要职务和社会兼职表

     */

    val academic_ds1 = spark.read.jdbc(url1, "academic_job", prop1)
    val academic_ds2 = academic_ds1.select("talent_id", "organization_name", "duty", "start_time", "end_time").
      na.drop(Array("organization_name", "start_time", "end_time")).filter($"organization_name" =!= "无").
      withColumn("talent_id", to_lower_udf($"talent_id")).
      na.fill(Map("duty" -> "无")).
      dropDuplicates()

    val academic_ds3 = academic_ds2.withColumn("time_diff", time_diff_udf($"start_time", $"end_time")).
      filter($"time_diff" >= 0).drop("time_diff")

    val col_academic_temp1 = Seq("id_1", "organization_name", "duty_1", "s1", "e1")
    val academic_temp1 = academic_ds3.toDF(col_academic_temp1: _*)
    val col_academic_temp2 = Seq("id_2", "organization_name", "duty_2", "s2", "e2")
    val academic_temp2 = academic_ds3.toDF(col_academic_temp2: _*)

    val academic_ds4 = academic_temp1.join(academic_temp2, Seq("organization_name"), "outer").
      filter($"id_1" =!= $"id_2")

    def academic_degree_duty(t1: String, t2: String): Double = {
      val result = if (t1 == t2 & t1 != "无" & t2 != "无") {
        1.0
      } else {
        0.0
      }
      return result * 100
    }
    val academic_degree_duty_udf = udf((title_1: String, title_2: String) => academic_degree_duty(title_1, title_2))

    def academic_degree_date(s1: String, e1: String, s2: String, e2: String): Double = {
      //定义时间格式
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val s1_L = dateFormat.parse(s1).getTime
      val s2_L = dateFormat.parse(s2).getTime
      val e1_L = dateFormat.parse(e1).getTime
      val e2_L = dateFormat.parse(e2).getTime

      val diff_1 = (e1_L - s1_L) / (1000 * 3600 * 24) + 1
      val diff_2 = (e2_L - s2_L) / (1000 * 3600 * 24) + 1
      val mean_diff = (diff_2 + diff_1) / 2.0

      val latest_start = max(s1_L, s2_L)
      val earliest_end = min(e1_L, e2_L)

      val overlap = ((earliest_end - latest_start) / (1000 * 3600 * 24) + 1) / mean_diff

      val result = if (overlap < 0) {
        0.0
      } else {
        (overlap * 100).toDouble
      }
      return result
    }
    val academic_degree_date_udf = udf((s1: String, e1: String, s2: String, e2: String) => academic_degree_date(s1, e1, s2, e2))

    val academic_ds5 = academic_ds4.withColumn("degree_duty", academic_degree_duty_udf($"duty_1", $"duty_2")).
      withColumn("degree_date", academic_degree_date_udf($"s1", $"e1", $"s2", $"e2")).
      withColumn("degree", bround($"degree_duty" * 0.5 + $"degree_date" * 0.5, 3)) // 保留三位有效数字

    val academic_col = Seq("id_1","id_2","degree_academic")
    val academic_ds6 = academic_ds5.select("id_1", "id_2", "degree").toDF(academic_col:_*)
    val academic_ds7 = academic_ds5.select("id_2", "id_1", "degree").toDF(academic_col:_*)
    val academic_ds8 = academic_ds6.union(academic_ds7).
      groupBy("id_1","id_2").agg(f_max($"degree_academic")).
      drop("degree_academic").toDF(academic_col:_*)

//        println(academic_ds8.count())
//        println(academic_ds8.dropDuplicates().count())
//        println(academic_ds8.select("id_1","id_2").dropDuplicates().count())
//        println(academic_ds8.groupBy("id_1","id_2").agg(f_max($"degree_academic")).count())

    /*
    3. 参加会议情况表

     */

    val meeting_ds1 = spark.read.jdbc(url1, "meeting_info", prop1)
    val meeting_ds2 = meeting_ds1.select("talent_id", "meeting_name", "start_time", "end_time").
      na.drop().filter($"meeting_name" =!= "无").
      withColumn("talent_id", to_lower_udf($"talent_id")).
      dropDuplicates()

    val meeting_ds3 = meeting_ds2.withColumn("time_diff", time_diff_udf($"start_time", $"end_time")).
      filter($"time_diff" >= 0).drop("time_diff")

    val col_meeting_temp1 = Seq("id_1", "meeting_name", "s1", "e1")
    val meeting_temp1 = meeting_ds3.toDF(col_meeting_temp1: _*)
    val col_meeting_temp2 = Seq("id_2", "meeting_name", "s2", "e2")
    val meeting_temp2 = meeting_ds3.toDF(col_meeting_temp2: _*)

    val meeting_ds4 = meeting_temp1.join(meeting_temp2, Seq("meeting_name"), "outer").
      filter($"id_1" =!= $"id_2")

    def meeting_degree_date(s1: String, e1: String, s2: String, e2: String): Double = {
      //定义时间格式
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val s1_L = dateFormat.parse(s1).getTime
      val s2_L = dateFormat.parse(s2).getTime
      val e1_L = dateFormat.parse(e1).getTime
      val e2_L = dateFormat.parse(e2).getTime

      val diff_1 = (e1_L - s1_L) / (1000 * 3600 * 24) + 1
      val diff_2 = (e2_L - s2_L) / (1000 * 3600 * 24) + 1
      val mean_diff = (diff_2 + diff_1) / 2.0

      val latest_start = max(s1_L, s2_L)
      val earliest_end = min(e1_L, e2_L)

      val overlap = ((earliest_end - latest_start) / (1000 * 3600 * 24) + 1) / mean_diff
      val result = if (overlap < 0) {
        0.0
      } else {
        (overlap * 100).toDouble
      }
      return result
    }
    val meeting_degree_date_udf = udf((s1: String, e1: String, s2: String, e2: String) => meeting_degree_date(s1, e1, s2, e2))

    val meeting_ds5 = meeting_ds4.withColumn("degree", meeting_degree_date_udf($"s1", $"e1", $"s2", $"e2"))

    val meeting_col = Seq("id_1","id_2","degree_meeting")
    val meeting_ds6 = meeting_ds5.select("id_1", "id_2", "degree").toDF(meeting_col:_*)
    val meeting_ds7 = meeting_ds5.select("id_2", "id_1", "degree").toDF(meeting_col:_*)
    val meeting_ds8 = meeting_ds6.union(meeting_ds7).
      groupBy("id_1","id_2").agg(f_max($"degree_meeting")).
      drop("degree_meeting").toDF(meeting_col:_*)

//    println(meeting_ds8.count())
//    println(meeting_ds8.dropDuplicates().count())
//    println(meeting_ds8.select("id_1","id_2").dropDuplicates().count())
//    println(meeting_ds8.groupBy("id_1","id_2").agg(f_max($"degree_meeting")).count())

    /*
    4. 享受人才工程表

     */

    val project_ds1 = spark.read.jdbc(url1, "talent_project_fund", prop1)
    val project_ds2 = project_ds1.select("talent_id", "project_name", "start_time", "end_time").
      na.drop().
      withColumn("talent_id", to_lower_udf($"talent_id")).
      dropDuplicates()

    val project_ds3 = project_ds2.withColumn("time_diff", time_diff_udf($"start_time", $"end_time")).
      filter($"time_diff" >= 0).drop("time_diff")

    val col_project_temp1 = Seq("id_1", "project_name", "s1", "e1")
    val project_temp1 = project_ds3.toDF(col_project_temp1: _*)
    val col_project_temp2 = Seq("id_2", "project_name", "s2", "e2")
    val project_temp2 = project_ds3.toDF(col_project_temp2: _*)

    val project_ds4 = project_temp1.join(project_temp2, Seq("project_name"), "outer").
      filter($"id_1" =!= $"id_2")

    def project_degree_date(s1: String, e1: String, s2: String, e2: String): Double = {
      //定义时间格式
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val s1_L = dateFormat.parse(s1).getTime
      val s2_L = dateFormat.parse(s2).getTime
      val e1_L = dateFormat.parse(e1).getTime
      val e2_L = dateFormat.parse(e2).getTime

      val diff_1 = (e1_L - s1_L) / (1000 * 3600 * 24) + 1
      val diff_2 = (e2_L - s2_L) / (1000 * 3600 * 24) + 1
      val mean_diff = (diff_2 + diff_1) / 2.0

      val latest_start = max(s1_L, s2_L)
      val earliest_end = min(e1_L, e2_L)

      val overlap = ((earliest_end - latest_start) / (1000 * 3600 * 24) + 1) / mean_diff

      val result = if (overlap < 0) {
        0.0
      } else {
        (overlap * 100).toDouble
      }
      return result
    }
    val project_degree_date_udf = udf((s1: String, e1: String, s2: String, e2: String) => project_degree_date(s1, e1, s2, e2))

    val project_ds5 = project_ds4.withColumn("degree", bround(project_degree_date_udf($"s1", $"e1", $"s2", $"e2"), 3))

    val project_col = Seq("id_1","id_2","degree_project")
    val project_ds6 = project_ds5.select("id_1", "id_2", "degree").toDF(project_col:_*)
    val project_ds7 = project_ds5.select("id_2", "id_1", "degree").toDF(project_col:_*)
    val project_ds8 = project_ds6.union(project_ds7).
      groupBy("id_1","id_2").agg(f_max($"degree_project")).
      drop("degree_project").toDF(project_col:_*)

//    println(project_ds8.count())
//    println(project_ds8.dropDuplicates().count())
//    println(project_ds8.select("id_1","id_2").dropDuplicates().count())
//    println(project_ds8.groupBy("id_1","id_2").agg(f_max($"degree_project")).count())
//

    /*
val url2 = "jdbc:mysql://10.20.7.156:3306/rck?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
val prop2 = new Properties()
prop2.setProperty("user", "root")
prop2.setProperty("password", "rcDsj_56")
*/
    val url2 = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    //    val url = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8&" +
    //      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    // 使用"useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"防止出现时间上的错误
    val prop2 = new Properties()
    prop2.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")


//    val ds1 = spark.read.jdbc(url2, "relation_new", prop2).
    val ds1 = spark.read.jdbc(url2, "relation", prop2).
      select("source_id","target_id","relation","weight").
      filter($"weight" > 0)

    val ds2_xiaoyou = ds1.filter($"relation" === "校友").
      toDF("id_1","id_2","relation","degree_xiaoyou").drop("relation")

    val ds2_tongshi = ds1.filter($"relation" === "同事").
      toDF("id_1","id_2","relation","degree_tongshi").drop("relation")

    val ds3 = profession_ds7.join(academic_ds8, Seq("id_1","id_2"), "outer").
      join(meeting_ds8, Seq("id_1","id_2"), "outer").
      join(project_ds8, Seq("id_1","id_2"), "outer").
      join(ds2_xiaoyou, Seq("id_1","id_2"), "outer").
      join(ds2_tongshi, Seq("id_1","id_2"), "outer").
      na.fill(value = 0.0, cols = Array("degree_profession","degree_academic","degree_meeting","degree_project","degree_xiaoyou", "degree_tongshi")).
      withColumn("relation",lit("熟悉程度")).
      withColumn("relation_object", lit(null).cast(StringType))


    val ds3_temp = profession_ds7.join(academic_ds8, Seq("id_1","id_2"), "outer").
      join(meeting_ds8, Seq("id_1","id_2"), "outer").
      join(project_ds8, Seq("id_1","id_2"), "outer").
      na.fill(value = 0.0, cols = Array("degree_profession","degree_academic","degree_meeting","degree_project")).
      withColumn("relation",lit("熟悉程度")).
      withColumn("relation_object", lit(null).cast(StringType))

//    println(ds3_temp.count())
//    println(ds3_temp.dropDuplicates().count())
//    println(ds3_temp.select("id_1","id_2").dropDuplicates().count())
//
//
    ds3_temp.coalesce(10).write.mode("overwrite").jdbc(url2, "relation_shuxi_temp", prop2) //overwrite;append
//    ds3_temp.write.mode("overwrite").jdbc(url2, "relation_shuxi_temp", prop2) //overwrite;append

    /*
    select count(*) from relation_shuxi_temp
    3469076
     */

    // 获取时间差
//    val d3 = new java.util.Date()
//    println("计算相似性时间： " + d3.getTime)
//    val diff = d3.getTime - d0.getTime // 返回自此Date对象表示的1970年1月1日，00:00:00 GMT以来的毫秒数。
//    val diffMinutes = diff / (1000.0) // 时间间隔，单位：秒
//    println("时间间隔(单位：秒): " + diffMinutes)
//    println("时间间隔(单位：分钟): " + diffMinutes / 60.0) // 6.729299999999999

    /*
    def mean_func(x1:Double, x2:Double,x3:Double,x4:Double, x5:Double,x6:Double):Double={
      val arr1 = Array(x1,x2,x3,x4,x5,x6)
      val sum_value = arr1.sum
      val mean_value = sum_value / arr1.length
      return mean_value
    }
    val mean_udf = udf((x1:Double, x2:Double,x3:Double,x4:Double, x5:Double,x6:Double) => mean_func(x1,x2,x3,x4,x5,x6))

    val ds4 = ds3.withColumn("weight", bround(mean_udf($"degree_profession",$"degree_academic",$"degree_meeting",$"degree_project",$"degree_xiaoyou",$"degree_tongshi"),3))

    val info_ds = spark.read.jdbc(url1, "talent_info_new", prop1).
      select("talent_id","name").
      withColumn("talent_id", to_lower_udf($"talent_id")).
      dropDuplicates()

    val info_id1 = info_ds.toDF("id_1","name_1")
    val info_id2 = info_ds.toDF("id_2","name_2")

    val ds5 = ds4.join(info_id1,Seq("id_1"),"left").
      join(info_id2,Seq("id_2"),"left")
    val ds6 = ds5.withColumn("create_time", current_timestamp()).
      withColumn("create_time", date_format($"create_time", "yyyy-MM-dd HH:mm:ss")).
      withColumn("update_time",$"create_time")

    val result_col = Seq("source_id","source_name","target_id","target_name","relation","relation_object","weight","create_time","update_time")

    val result_ds = ds6.select("id_1","name_1","id_2","name_2","relation","relation_object","weight","create_time","update_time").
      toDF(result_col:_*).dropDuplicates.na.drop(Array("source_id","source_name","target_id","target_name"))

    result_ds.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //将结果保存到数据框中
    result_ds.coalesce(10).write.mode("append").jdbc(url2, "relation_shuxi", prop2) //overwrite

//    result_ds.write.mode("append").jdbc(url2, "relation_shuxi", prop2) //overwrite

*/
    sc.stop()
    spark.stop()
  }
}
