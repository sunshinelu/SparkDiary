package com.evayInfo.Inglory.Project.Recommend.AppRecom

import java.util.Properties

import com.evayInfo.Inglory.Project.Recommend.RecomUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/8/25.
 */
object UsersimiSub {

  case class UserSimi(userId1: Long, userId2: Long, similar: Double)

  def main(args: Array[String]) {
    RecomUtil.SetLogger

    val SparkConf = new SparkConf().setAppName(s"UsersimiSub").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    val ylzxTable = args(0)
    val logsTable = args(1)
    val outputTable = args(2)
*/
    val ylzxTable = "yilan-total_webpage"
    val logsTable = "t_hbaseSink"
    val outputTable = "recommender_user_sub"


    val url1 = "jdbc:mysql://localhost:3306/ylzx?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver")
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "YLZX_NRGL_MYSUB_WEBSITE_COL", prop1)
    //    ds1.printSchema()
    /*
    root
 |-- OPERATOR_ID: string (nullable = false)
 |-- WEBSITE_ID: string (nullable = false)
 |-- COLUMN_ID: string (nullable = false)
 |-- COLUMN_NAME: string (nullable = true)
 |-- COLUMN_URL: string (nullable = true)
     */
    //    println("ds1 is: " + ds1.count())

    val ds2 = ds1.select("OPERATOR_ID", "COLUMN_ID").withColumn("value", lit(1)).na.drop()
    ds2.printSchema()
    println("ds2 is: " + ds2.count())
    //string to number
    val userID = new StringIndexer().setInputCol("OPERATOR_ID").setOutputCol("userID").fit(ds2)
    val ds3 = userID.transform(ds2)
    val urlID = new StringIndexer().setInputCol("COLUMN_ID").setOutputCol("urlID").fit(ds3)
    val ds4 = urlID.transform(ds3)
    ds4.printSchema()

    val ds5 = ds4.withColumn("userID", ds4("userID").cast("long")).
      withColumn("urlID", ds4("urlID").cast("long")).
      withColumn("value", ds4("value").cast("double"))
    ds5.printSchema()

    val user1Lab = ds5.select("userID", "OPERATOR_ID").withColumnRenamed("userID", "user1Id").withColumnRenamed("OPERATOR_ID", "user1").na.drop().dropDuplicates()
    val user2Lab = ds5.select("userID", "OPERATOR_ID").withColumnRenamed("userID", "user2Id").withColumnRenamed("OPERATOR_ID", "user2").na.drop().dropDuplicates()

    //RDD to RowRDD
    val rdd1 = ds5.select("userID", "urlID", "value").rdd.
      map { case Row(user: Long, item: Long, value: Double) => MatrixEntry(user, item, value) }

    //calculate similarities
    val ratings = new CoordinateMatrix(rdd1).transpose()
    val userSimi = ratings.toRowMatrix.columnSimilarities(0.1)
    // user-user similarity
    val userSimiRdd = userSimi.entries.map(f => UserSimi(f.i, f.j, f.value))

    // user1, user1, similar
    val userSimiDF = userSimiRdd.map { f => (f.userId1, f.userId2, f.similar) }.
      union(userSimiRdd.map { f => (f.userId2, f.userId1, f.similar) }).toDF("user1Id", "user2Id", "userSimi")

    val simiJoined = userSimiDF.join(user1Lab, Seq("user1Id"), "left").join(user2Lab, Seq("user2Id"), "left").
      select("user1","user2","userSimi")
    simiJoined.take(5).foreach(println)
    simiJoined.printSchema()
    /*
root
 |-- user2Id: long (nullable = true)
 |-- user1Id: long (nullable = true)
 |-- userSimi: double (nullable = true)
 |-- user1: string (nullable = true)
 |-- user2: string (nullable = true)
     */

    val ylzxRDD = RecomUtil.getYlzxRDD(ylzxTable, sc)
    val ylzxDF = spark.createDataset(ylzxRDD).dropDuplicates("content").drop("content")
    val logsRDD = RecomUtil.getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString")).select("userString","itemString", "value")

    val userR_1 = simiJoined.join(logsDS, simiJoined("user1") === logsDS("userString"), "left").
      withColumn("recomValue", col("userSimi") * col("value")).
      groupBy("user2", "itemString").agg(sum($"recomValue")).
      withColumnRenamed("sum(recomValue)","recomValue")

    val logsDS2  = logsDS.select("userString", "itemString").withColumnRenamed("userString", "user2").withColumn("whether",lit(1))
    val userR_2 = userR_1.join(logsDS2, Seq("user2", "itemString"), "left").filter(col("whether").isNull)
    userR_2.printSchema()
    /*
    root
 |-- user2: string (nullable = true)
 |-- itemString: string (nullable = true)
 |-- recomValue: double (nullable = true)
 |-- whether: integer (nullable = true)
     */
    userR_2.take(4).foreach(println)

    //(itemString: String, title: String, manuallabel: String, time: String, websitename: String, content: String)
    val userR_3 = userR_2.join(ylzxDF, Seq("itemString"), "left")

    //对dataframe进行分组排序，并取每组的前5个
    val w = Window.partitionBy("user2").orderBy(col("recomValue").desc)
    val userR_4 = userR_3.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val userR_5 = userR_4.select("user2", "itemString", "recomValue", "rn", "title", "manuallabel", "time")
      .withColumn("systime", current_timestamp()).withColumn("systime", date_format($"systime", "yyyy-MM-dd HH:mm:ss"))

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围

    /*
    如果outputTable表存在，则删除表；如果不存在则新建表。
     */
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable)) {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(outputTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)

    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名

    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    userR_5.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))).
      map(x => {
        val userString = x._1.toString
        val itemString = x._2.toString
        //保留rating有效数字
        val rating = x._3.toString.toDouble
        val rating2 = f"$rating%1.5f".toString
        val rn = x._4.toString
        val title = if (null != x._5) x._5.toString else ""
        val manuallabel = if (null != x._6) x._6.toString else ""
        val time = if (null != x._7) x._7.toString else ""
        val sysTime = if (null != x._8) x._8.toString else ""
        (userString, itemString, rating2, rn, title, manuallabel, time, sysTime)
      }).filter(_._5.length >= 2).
      map { x => {
        val paste = x._1 + "::score=" + x._4.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("userID"), Bytes.toBytes(x._1.toString)) //标签的family:qualify,userID
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._2.toString)) //id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(x._3.toString)) //rating
        put.add(Bytes.toBytes("info"), Bytes.toBytes("rn"), Bytes.toBytes(x._4.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("title"), Bytes.toBytes(x._5.toString)) //title
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._6.toString)) //manuallabel
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._7.toString)) //mod
        put.add(Bytes.toBytes("info"), Bytes.toBytes("sysTime"), Bytes.toBytes(x._8.toString)) //sysTime

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)
    /*
        val ds6_0 = userSimiDF.withColumn("userID", col("userID").cast("double")).
          withColumn("urlID", col("urlID").cast("double"))


        val indexer1 = new IndexToString()
          .setInputCol("userID")
          .setOutputCol("userString")

        val ds6 = indexer1.transform(ds6_0)


        val indexer2 = new IndexToString()
          .setInputCol("urlID")
          .setOutputCol("urlString")

        val ds7 = indexer2.transform(ds6)
        ds7.printSchema()


        val ds8 = ds7.join(userLab, Seq("userID"), "left").join(itemLab, Seq("urlID"), "left")
        ds8.printSchema()
        ds8.take(5).foreach(println)
    */

    sc.stop()
    spark.stop()
  }

}
