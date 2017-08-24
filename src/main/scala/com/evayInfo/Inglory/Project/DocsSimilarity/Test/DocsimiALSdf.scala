package com.evayInfo.Inglory.Project.DocsSimilarity.Test

import com.evayInfo.Inglory.Project.DocsSimilarity.DocsimiUtil
import com.evayInfo.Inglory.Project.Recommend.itemModel._
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/8/24.
 *
 * 使用spark版本为spark2.2.0
 *
 */
object DocsimiALSdf {
  def main(args: Array[String]): Unit = {
    DocsimiUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiALS").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val ylzxTable = "yilan-total_webpage" //args(0)
    val logsTable = "t_hbaseSink" //args(1)
    val outputTable = "docsimi_als" //args(2)

    val ylzxRDD = getYlzxRDD(ylzxTable, sc)
    val ylzxDF = spark.createDataset(ylzxRDD).dropDuplicates("content").drop("content")

    val logsRDD = getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString"))
    val ds1 = logsDS.groupBy("userString", "itemString").agg(sum("value")).withColumnRenamed("sum(value)", "rating")
    //string to number
    val userID = new StringIndexer().setInputCol("userString").setOutputCol("userID").fit(ds1)
    val ds2 = userID.transform(ds1)
    val urlID = new StringIndexer().setInputCol("itemString").setOutputCol("urlID").fit(ds2)
    val ds3 = urlID.transform(ds2)

    // Build the recommendation model using ALS
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userID")
      .setItemCol("urlID")
      .setRatingCol("rating")
    val alsModel = als.fit(ds3)

    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    // alsModel.setColdStartStrategy("drop")//spark2.2.0以上才可以使用该方法
    val predictions = alsModel.transform(ds3)

    // Generate top 10 item recommendations for each user
    //    val userRecs = alsModel.recommendForAllUsers(10)
    //    userRecs.printSchema()
    /*
    root
 |-- userID: integer (nullable = false)
 |-- recommendations: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- urlID: integer (nullable = true)
 |    |    |-- rating: float (nullable = true)
     */
    //    userRecs.show(5)
    /*
+------+--------------------+
|userID|     recommendations|
+------+--------------------+
|    31|[[310,16.64283], ...|
|    34|[[9,5.1186066], [...|
|    28|[[35,3.1186228], ...|
|    26|[[35,1.2680218], ...|
|    27|[[35,5.3822002], ...|
+------+--------------------+
 */
    //    userRecs.take(5).foreach(println)
    /*
[31,WrappedArray([310,16.64283], [9,12.308903], [507,10.740815], [120,10.67602], [627,7.464513], [541,6.4125767], [148,5.9977674], [20,5.5277777], [1511,5.281918], [787,4.0494704])]
[34,WrappedArray([9,5.1186066], [35,3.198337], [20,1.7278893], [69,1.5137966], [535,1.3711632], [2694,1.2245307], [6,1.0040171], [55,0.8872187], [8,0.5099099], [230,0.43581432])]
[28,WrappedArray([35,3.1186228], [69,1.6264027], [2694,1.2263823], [132,1.2071085], [614,0.9294646], [2474,0.9130217], [8,0.57166785], [2534,0.4698782], [149,0.46684474], [230,0.36850023])]
[26,WrappedArray([35,1.2680218], [69,0.6768039], [2474,0.56734896], [2694,0.5140675], [132,0.45543808], [561,0.44054833], [2534,0.4378014], [149,0.3209739], [8,0.29510733], [614,0.29338822])]
[27,WrappedArray([35,5.3822002], [310,5.3034353], [69,2.5897698], [2694,2.1112096], [9,1.9663877], [55,1.7160919], [541,0.96686625], [20,0.84550387], [230,0.8321134], [2534,0.7021123])]

     */
    // Generate top 10 user recommendations for each item
    //    val itemRecs = alsModel.recommendForAllItems(10)
    //    itemRecs.printSchema()
    /*
    root
     |-- urlID: integer (nullable = false)
     |-- recommendations: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- userID: integer (nullable = true)
     |    |    |-- rating: float (nullable = true)

     */
    //    itemRecs.show(5)
    /*
    +-----+--------------------+
    |urlID|     recommendations|
    +-----+--------------------+
    | 1580|[[6,0.09777494], ...|
    |  471|[[0,0.1436925], [...|
    | 1591|[[0,0.10166461], ...|
    | 1342|[[7,0.3714695], [...|
    | 2122|[[23,0.19061372],...|
    +-----+--------------------+
     */
    //    itemRecs.take(5).foreach(println)
    /*
    [1580,WrappedArray([6,0.09777494], [14,0.039981514], [23,0.03893248], [8,0.023851618], [2,0.023032332], [3,0.022670817], [28,0.020578856], [12,0.01954649], [20,0.01819141], [30,0.01722509])]
    [471,WrappedArray([0,0.1436925], [7,0.1209123], [13,0.120648324], [30,0.09101435], [27,0.07865646], [8,0.07009581], [9,0.06520276], [10,0.06086657], [5,0.059293102], [29,0.052750647])]
    [1591,WrappedArray([0,0.10166461], [18,0.053118005], [29,0.026249874], [13,0.023917092], [30,0.023745881], [27,0.02132535], [12,0.020513305], [16,0.018863678], [10,0.01577449], [17,0.015135724])]
    [1342,WrappedArray([7,0.3714695], [5,0.26788026], [14,0.23828207], [11,0.21719718], [8,0.21144632], [27,0.21009737], [13,0.20604898], [15,0.20286503], [0,0.18674086], [9,0.17485294])]
    [2122,WrappedArray([23,0.19061372], [6,0.15908019], [3,0.15397176], [20,0.14341992], [14,0.122763656], [30,0.11884755], [5,0.11786503], [28,0.11416218], [8,0.10630525], [13,0.100594334])]

     */
    sc.stop()
    spark.stop()
  }
}
