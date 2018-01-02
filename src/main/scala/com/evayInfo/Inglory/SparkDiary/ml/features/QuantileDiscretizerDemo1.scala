package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/1/2.
 *
 * * 参考链接：
 * spark ML 中 VectorIndexer, StringIndexer等用法
 * http://blog.csdn.net/shenxiaoming77/article/details/63715525
 *
 *
 * 分位树为数离散化，和Bucketizer（分箱处理）一样也是：将连续数值特征转换为离散类别特征。实际上Class QuantileDiscretizer extends （继承自） Class（Bucketizer）。

-参数1：不同的是这里不再自己定义splits（分类标准），而是定义分几箱(段）就可以了。QuantileDiscretizer自己调用函数计算分位数，并完成离散化。
-参数2： 另外一个参数是精度，如果设置为0，则计算最精确的分位数，这是一个高时间代价的操作。另外上下边界将设置为正负无穷，覆盖所有实数范围。

QuantileDiscretizer takes a column with continuous features and outputs a column with binned categorical features.
The number of bins is set by the numBuckets parameter. The bin ranges are chosen using an approximate algorithm (see the documentation for approxQuantile for a detailed description).
The precision of the approximation can be controlled with the relativeError parameter. When set to zero, exact quantiles are calculated (Note: Computing exact quantiles is an expensive operation).
The lower and upper bin bounds will be -Infinity and +Infinity covering all real values.

 *
 */
object QuantileDiscretizerDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"QuantileDiscretizerDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = spark.createDataFrame(data).toDF("id", "hour")

    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3)//设置分箱数
      .setRelativeError(0.1) //设置precision-控制相对误差

    val result = discretizer.fit(df).transform(df)
    result.show(false)
    /*
+---+----+------+
|id |hour|result|
+---+----+------+
|0  |18.0|2.0   |
|1  |19.0|2.0   |
|2  |8.0 |1.0   |
|3  |5.0 |1.0   |
|4  |2.2 |0.0   |
+---+----+------+
     */



    sc.stop()
    spark.stop()

  }
}
