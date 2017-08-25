package com.evayInfo.Inglory.TestCode

import java.sql.{Connection, DriverManager}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/25.
 */
object Test003sun {

  case class Person1(id: Int, ch_name: String, intro: String)

  case class Person2(rc_id: String, ch_name: String, work_unit: String)

  case class insert(id: Int, rc_id: String)

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {

    SetLogger

    val spark = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()
    import spark.implicits._

    val df1 = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://172.16.10.141:3306/sunhao?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "tmp_cjxz",
        "user" -> "root",
        "password" -> "123456")).load()
    val df2 = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://172.16.10.141:3306/sunhao?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "rc_jbxx",
        "user" -> "root",
        "password" -> "123456")).load()

    println("df1 is: ")
    df1.printSchema()

    val df3 = df2.filter($"source" === "长江学者").select("rc_id", "ch_name", "work_unit")
    df3.createOrReplaceTempView("people")
    println("df3 is: ")
    println(df3.count())
    df2.printSchema()

    val sqlDF = spark.sql("select * from people where ch_name in (select ch_name from people group by ch_name having count(ch_name) > 1)")
    val df4 = sqlDF.as[Person2]
    println("df4 is: ")
    df4.printSchema()

    val ds1 = df1.select("id", "ch_name", "intro").as[Person1]
    println("ds1 is: ")
    ds1.printSchema()

    val res_ds1 = ds1.join(df4, Seq("ch_name"), "left")
//    .join(df3, col("intro") === col("work_unit"), "inner")
      .filter($"intro".contains($"work_unit"))
      .select("id", "rc_id")
    // .map(d=> insert(d(0),d(1)))
    //      .map{case Row(id:Int,rc_id:String) => updateTable(id,rc_id)}

    res_ds1.printSchema()

    spark.stop()
  }

  //创建mysql连接
  def getMysqlConn(): Connection = {

    val mysqlConfTest = collection.mutable.Map(
      "driver" -> "com.mysql.jdbc.Driver",
      "url" -> "jdbc:mysql://172.16.10.141:3306/sunhao?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC",
      "username" -> "root",
      "password" -> "123456"
    )
    Class.forName(mysqlConfTest("driver"))
    DriverManager.getConnection(mysqlConfTest("url"), mysqlConfTest("username"), mysqlConfTest("password"))
  }

  //更新mysql的数据
  def updateTable(id: Int, rc_id: String): Unit = {
    // create database connection
    val conn = getMysqlConn
    try {
      val ps = conn.prepareStatement("UPDATE tmp_cjxz SET rc_id = ? WHERE id = ?")
      // set the preparedstatement parameters
      ps.setString(1, rc_id)
      ps.setInt(2, id)

      // call executeUpdate to execute our sql update statement
      val res = ps.executeUpdate()
      ps.close()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      conn.close
    }
  }

}
