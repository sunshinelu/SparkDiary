package com.evayInfo.Inglory.Project.Recommend.Wu

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Created by sunlu on 17/10/19.
 * 吴子宾构建构建推荐模型思路
 * als recommend model + item-based recommend model
 * (In item-based model, using als model generate doc features, and then using cosine calculate doc-doc similarity)
 *
spark-submit --class com.evayInfo.Inglory.Project.Recommend.Wu.WuRecommender \
--master yarn \
--num-executors 6 \
--executor-cores 4 \
--executor-memory 8 \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/Test/SparkDiary.jar

 * 运行成功！ 运行时间：1hrs, 44mins, 2sec
 */
object WuRecommender {

  case class docSimsSchema(doc1ID: Long, doc2ID: Long, sims: Double)

  case class TopProductsSchema(userID: Long, urlID: Long, als_rating: Double)

  def main(args: Array[String]) {

    WuUtil.SetLogger

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)

    // bulid spark environment
    val sparkConf = new SparkConf().setAppName(s"WuRecommender") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = "yilan-total-analysis_webpage"
    val logsTable = "t_hbaseSink"
    val outputTable = "wu_recommend"

    val logsRDD = WuUtil.getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString"))
    val ylzxRDD = WuUtil.getYlzxYRDD2(ylzxTable, 20, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).dropDuplicates("content").drop("content")

    val ds1 = logsDS.groupBy("userString", "itemString").agg(sum("value")).withColumnRenamed("sum(value)", "rating")

    //string to number
    val userID = new StringIndexer().setInputCol("userString").setOutputCol("userID").fit(ds1)
    val ds2 = userID.transform(ds1)
    val urlID = new StringIndexer().setInputCol("itemString").setOutputCol("urlID").fit(ds2)
    val ds3 = urlID.transform(ds2)

    val ds4 = ds3.withColumn("userID", ds3("userID").cast("long")).
      withColumn("urlID", ds3("urlID").cast("long")).
      withColumn("rating", ds3("rating").cast("double"))

    ds4.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //Min-Max Normalization[0,1]
    val minMax = ds4.agg(max("rating"), min("rating")).withColumnRenamed("max(rating)", "max").withColumnRenamed("min(rating)", "min")
    val maxValue = minMax.select("max").rdd.map { case Row(d: Double) => d }.first()
    val minValue = minMax.select("min").rdd.map { case Row(d: Double) => d }.first
    //limit the values to 4 digit
    val ds5 = ds4.withColumn("norm", bround(((ds4("rating") - minValue) / (maxValue - minValue)), 4))

    ds5.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ds4.unpersist()

    //RDD to RowRDD
    val alsRDD = ds5.select("userID", "urlID", "norm").rdd.map { row => (row(0), row(1), row(2)) }.map { x =>
      val user = x._1.toString.toInt
      val item = x._2.toString.toInt
      val rate = x._3.toString.toDouble
      Rating(user, item, rate)
    }

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val alsModel = ALS.train(alsRDD, rank, numIterations, 0.01)

    /*
    als model result
     */
    val topProducts = alsModel.recommendProductsForUsers(15)
    val topProductsRowRDD = topProducts.flatMap(x => {
      val y = x._2
      for (w <- y) yield (w.user, w.product, w.rating)
    }).map { f => TopProductsSchema(f._1.toLong, f._2.toLong, f._3) }

    val topProductsDF = spark.createDataset(topProductsRowRDD)

    topProductsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val (als_Min, als_Max) = topProductsDF.agg(min($"als_rating"), max($"als_rating")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }

    val scaledRange = lit(1) // Range of the scaled variable
    val scaledMin = lit(0) // Min value of the scaled variable
    val als_Normalized = ($"als_rating" - als_Min) / (als_Max - als_Min) // v normalized to (0, 1) range

    val als_Scaled = scaledRange * als_Normalized + scaledMin

    val topProductsDF_norm = topProductsDF.withColumn("als_norm", bround(als_Scaled, 4)).drop("als_rating")

    topProductsDF.unpersist()

    /*
    item-based recommender
     */
    // generate doc features
    val productFeatures = alsModel.productFeatures.map {
      case (id: Int, vec: Array[Double]) => {
        val vector = Vectors.dense(vec)
        new IndexedRow(id, vector)
      }
    }

    /*
    using cosine calculate doc-doc similarity
     */
    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.5.toDouble
    val upper = 1.0

    val mat = new IndexedRowMatrix(productFeatures)
    val mat_t = mat.toCoordinateMatrix.transpose()
    val sim = mat_t.toRowMatrix.columnSimilarities(threshhold)
    //    val sim2 = sim.entries.map { case MatrixEntry(i, j, u) => (i, j, u) }

    val sim_threshhold = sim.entries.filter { case MatrixEntry(i, j, u) => u >= threshhold && u <= upper }
    val sim_threshhold2 = sim_threshhold.map { case MatrixEntry(i, j, u) => MatrixEntry(j, i, u) }
    val docsim = sim_threshhold.union(sim_threshhold2)

    val docSimsRDD = docsim.map { x => {
      val doc1 = x.i
      val doc2 = x.j
      val sims = x.value
      docSimsSchema(doc1, doc2, sims)
    }
    }
    val docSimsDS = spark.createDataset(docSimsRDD)

    val item_df1 = ds5.join(docSimsDS, ds5("urlID") === docSimsDS("doc1ID"), "left").na.drop().
      withColumn("score", col("norm") * col("sims")).
      groupBy("userID", "doc2ID").agg(sum("score")).withColumnRenamed("sum(score)", "item_rating")
    val item_df2 = ds5.select("userID", "urlID").withColumnRenamed("urlID", "doc2ID").
      withColumn("whether", lit(1))

    val item_df3 = item_df1.join(item_df2, Seq("userID", "doc2ID"), "left").
      filter(col("whether").isNull).drop("whether").withColumnRenamed("doc2ID", "urlID")

    item_df3.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val (item_Min, item_Max) = item_df3.agg(min($"item_rating"), max($"item_rating")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }

    val item_Normalized = ($"item_rating" - item_Min) / (item_Max - item_Min) // v normalized to (0, 1) range

    val item_Scaled = scaledRange * item_Normalized + scaledMin

    val item_df4 = item_df3.withColumn("item_norm", bround(item_Scaled, 4)).drop("item_rating")

    /*
    combine als model result and item-based model result
     */

    val combined_df1 = topProductsDF_norm.join(item_df4, Seq("userID", "urlID"), "outer").
      na.drop(Array("userID", "urlID")).na.fill(value = 0.0, cols = Array("als_norm", "item_norm")).
      withColumn("weight", $"als_norm" + $"item_norm").
      groupBy("userID", "urlID").agg(sum("weight")).drop("weight").
      withColumnRenamed("sum(weight)", "rating")


    // get doc information
    val userLab = ds5.select("userString", "userID").dropDuplicates
    val itemLab = ds5.select("itemString", "urlID").dropDuplicates

    val joinDF1 = combined_df1.join(userLab, Seq("userID"), "left")
    val joinDF2 = joinDF1.join(itemLab, Seq("urlID"), "left")
    val joinDF3 = joinDF2.join(ylzxDS, Seq("itemString"), "left").na.drop()

    // secondary sort
    val w = Window.partitionBy("userString").orderBy(col("rating").desc)
    val joinDF4 = joinDF3.withColumn("rn", row_number.over(w)).where($"rn" <= 10)
    val joinDF5 = joinDF4.select("userString", "itemString", "rating", "rn", "title", "manuallabel", "time")

    item_df3.unpersist()

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //如果outputTable表存在，则删除表；如果不存在则新建表。

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
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名，与输入是同一个表t_userProfileV1

    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    joinDF5.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6))).
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
        val sysTime = today
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
      }.saveAsNewAPIHadoopDataset(jobConf) //.saveAsNewAPIHadoopDataset(job.getConfiguration)

    ds5.unpersist()

    sc.stop()
    spark.stop()

  }

}
