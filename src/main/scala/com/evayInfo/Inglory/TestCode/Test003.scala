package com.evayInfo.Inglory.TestCode

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.SparkSession

object Test003 {

  case class Person1(id: Int, ch_name: String, intro: String)

  case class Person2(rc_id: String, ch_name: String, work_unit: String)

  case class insert(id: Int, rc_id: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()
    import spark.implicits._

    val df1 = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://172.16.10.141:3306/talentscout",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "tmp_cjxz",
        "user" -> "root",
        "password" -> "123456")).load()
    val df2 = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://172.16.10.141:3306/talentscout",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "rc_jbxx",
        "user" -> "root",
        "password" -> "123456")).load()

    val df3 = df2.filter($"source" === "长江学者").select("rc_id", "ch_name", "work_unit")
    df3.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people where ch_name in (select ch_name from people group by ch_name having count(ch_name) > 1)")
    val df4 = sqlDF.as[Person2]
    val ds1 = df1.select("id", "ch_name", "intro").as[Person1]

    val res_ds1 = df1.join(ds1, "ch_name")
      .filter($"intro".contains($"work_unit"))
      .select("id", "rc_id")
    // .map(d=> insert(d(0),d(1)))
    //      .map{case Row(id:Int,rc_id:String) => updateTable(id,rc_id)}


  }

  //创建mysql连接
  def getMysqlConn(): Connection = {

    val mysqlConfTest = collection.mutable.Map(
      "driver" -> "com.mysql.jdbc.Driver",
      "url" -> "jdbc:mysql://172.16.10.141:3306/talentscout",
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
