package com.evayInfo.Inglory.Project.DocsSimilarity

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, HashingTF, IDF, MinHashLSH}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/8/31.
 *
 * 使用文章标题计算文章相似性
 *
 * 使用近一年的1%的数据测试发现，使用Jaccard Distance得出的结果较全。
 *
 */
object DocsimiTitle {
  def main(args: Array[String]) {
    DocsimiUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiTitle") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val ylzxTable = args(0)
    val docSimiTable = args(1)

    /*

    val ylzxTable = "yilan-total_webpage"
    val docSimiTable = "docsimi_title"
     */

    val ylzxRDD = DocsimiUtil.getYlzxSegRDD(ylzxTable, 20, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).drop("segWords") //.randomSplit(Array(0.01, 0.99))(0)

    //    println(ylzxDS.count())
    //res5: Long = 981
    //    109363
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    //定义UDF
    //分词、停用词过滤
    def segWordsFunc(content: String): Seq[String] = {
      val seg = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.value.contains(word)).toSeq
      val result = seg match {
        case r if (r.length >= 1) => r
        case _ => Seq("") // Seq("null")
      }
      result
    }

    val segWordsUDF = udf((content: String) => segWordsFunc(content))

    val segDF = ylzxDS.withColumn("segWords", segWordsUDF(column("title"))) //.filter(!$"segWords".contains("null"))

    /*
    calculate tf-idf value
     */
    val hashingTF = new HashingTF().
      setInputCol("segWords").
      setOutputCol("rawFeatures") //.setNumFeatures(20000)

    val featurizedData = hashingTF.transform(segDF)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    /*
scala> rescaledData.printSchema
root
 |-- id: long (nullable = true)
 |-- urlID: string (nullable = true)
 |-- title: string (nullable = true)
 |-- label: string (nullable = true)
 |-- time: string (nullable = true)
 |-- websitename: string (nullable = true)
 |-- segWords: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- rawFeatures: vector (nullable = true)
 |-- features: vector (nullable = true)
     */
    //    rescaledData.select("id", "features").show()

    /*
using  Euclidean Distance calculate doc-doc similarity
     */
    val brp = new BucketedRandomProjectionLSH().
      setBucketLength(2.0).
      setNumHashTables(3).
      setInputCol("features").
      setOutputCol("brpValues")
    val brpModel = brp.fit(rescaledData)

    val brpTransformed = brpModel.transform(rescaledData).cache()
    val docsimi_brp = brpModel.approxSimilarityJoin(brpTransformed, brpTransformed, 5.0)
    //    docsimi_brp.count()
    //res4: Long = 618654
    //    docsimi_brp.printSchema()
    /*
    scala>     docsimi_brp.printSchema()
    root
     |-- datasetA: struct (nullable = false)
     |    |-- id: long (nullable = true)
     |    |-- urlID: string (nullable = true)
     |    |-- title: string (nullable = true)
     |    |-- label: string (nullable = true)
     |    |-- time: string (nullable = true)
     |    |-- websitename: string (nullable = true)
     |    |-- segWords: array (nullable = true)
     |    |    |-- element: string (containsNull = true)
     |    |-- rawFeatures: vector (nullable = true)
     |    |-- features: vector (nullable = true)
     |    |-- brpValues: array (nullable = true)
     |    |    |-- element: vector (containsNull = true)
     |-- datasetB: struct (nullable = false)
     |    |-- id: long (nullable = true)
     |    |-- urlID: string (nullable = true)
     |    |-- title: string (nullable = true)
     |    |-- label: string (nullable = true)
     |    |-- time: string (nullable = true)
     |    |-- websitename: string (nullable = true)
     |    |-- segWords: array (nullable = true)
     |    |    |-- element: string (containsNull = true)
     |    |-- rawFeatures: vector (nullable = true)
     |    |-- brpValues: array (nullable = true)
     |    |    |-- element: vector (containsNull = true)
     |    |-- features: vector (nullable = true)
     |-- distCol: double (nullable = true)
     */
    /*
using Jaccard Distance calculate doc-doc similarity
     */
    val mh = new MinHashLSH().
      setNumHashTables(3).
      setInputCol("features").
      setOutputCol("mhValues")
    val mhModel = mh.fit(rescaledData)

    // Feature Transformation
    val mhTransformed = mhModel.transform(rescaledData)
    val docsimi_mh = mhModel.approxSimilarityJoin(mhTransformed, mhTransformed, 1.0)
    //    docsimi_mh.count()
    //res3: Long = 518977
    //    docsimi_mh.printSchema()
    /*
    scala>     docsimi_mh.printSchema()
    root
     |-- datasetA: struct (nullable = false)
     |    |-- id: long (nullable = true)
     |    |-- urlID: string (nullable = true)
     |    |-- title: string (nullable = true)
     |    |-- label: string (nullable = true)
     |    |-- time: string (nullable = true)
     |    |-- websitename: string (nullable = true)
     |    |-- segWords: array (nullable = true)
     |    |    |-- element: string (containsNull = true)
     |    |-- rawFeatures: vector (nullable = true)
     |    |-- features: vector (nullable = true)
     |    |-- mhValues: array (nullable = true)
     |    |    |-- element: vector (containsNull = true)
     |-- datasetB: struct (nullable = false)
     |    |-- id: long (nullable = true)
     |    |-- urlID: string (nullable = true)
     |    |-- title: string (nullable = true)
     |    |-- label: string (nullable = true)
     |    |-- time: string (nullable = true)
     |    |-- websitename: string (nullable = true)
     |    |-- segWords: array (nullable = true)
     |    |    |-- element: string (containsNull = true)
     |    |-- rawFeatures: vector (nullable = true)
     |    |-- mhValues: array (nullable = true)
     |    |    |-- element: vector (containsNull = true)
     |    |-- features: vector (nullable = true)
     |-- distCol: double (nullable = true)
     */

    val colRenamed = Seq("doc1Id", "doc1", "doc2Id", "doc2", "doc2_title",
      "doc2_label", "doc2_websitename", "doc2_time", "distCol")

    val brpSimiDF = docsimi_brp.select("datasetA.id", "datasetA.urlID", "datasetB.id", "datasetB.urlID", "datasetB.title",
      "datasetB.label", "datasetB.websitename", "datasetB.time", "distCol").toDF(colRenamed: _*).
      filter($"doc1Id" =!= $"doc2Id")

    val mhSimiDF = docsimi_mh.select("datasetA.id", "datasetA.urlID", "datasetB.id", "datasetB.urlID", "datasetB.title",
      "datasetB.label", "datasetB.websitename", "datasetB.time", "distCol").toDF(colRenamed: _*).
      filter($"doc1Id" =!= $"doc2Id")

    //对dataframe进行分组排序，并取每组的前5个
    val w = Window.partitionBy("doc1Id").orderBy(col("distCol").asc)
    val brpSortedDF = brpSimiDF.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)
    //    brpSortedDF.count()
    //res6: Long = 4884
    val mhSortedDF = mhSimiDF.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)
    //    mhSortedDF.count()
    //res7: Long = 4905


    val resultDF = mhSortedDF.select("doc1", "doc2", "distCol", "rn", "doc2_title", "doc2_label", "doc2_time", "doc2_websitename")

    val hbaseConf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    //    hbaseConf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名

    /*
    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(hbaseConf)
    if (!hadmin.isTableAvailable(docSimiTable)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(docSimiTable))
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes()))
//      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }
*/

    //如果outputTable表存在，则删除表；如果不存在则新建表。=> START
    val hAdmin = new HBaseAdmin(hbaseConf)
    if (hAdmin.tableExists(docSimiTable)) {
      hAdmin.disableTable(docSimiTable)
      hAdmin.deleteTable(docSimiTable)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(docSimiTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)
    //如果outputTable表存在，则删除表；如果不存在则新建表。=> OVER

    //指定输出格式和输出表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, docSimiTable) //设置输出表名


    //    val table = new HTable(hbaseConf,docSimiTable)
    //    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,docSimiTable)

    val jobConf = new Configuration(hbaseConf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    resultDF.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))).
      map { x => {
        //("doc1", "doc2", "distCol", "rn", "doc2_title", "doc2_label", "doc2_time", "doc2_websitename")
        val paste = x._1.toString + "::score=" + x._4.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._1.toString)) //doc1
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsID"), Bytes.toBytes(x._2.toString)) //doc2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._3.toString)) //value
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._4.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("t"), Bytes.toBytes(x._5.toString)) //title2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._6.toString)) //label2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._7.toString)) //time2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._8.toString)) //websitename2

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)






    sc.stop()
    spark.stop()
  }

}
