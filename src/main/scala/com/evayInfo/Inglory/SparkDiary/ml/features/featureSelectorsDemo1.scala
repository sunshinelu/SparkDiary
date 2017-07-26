package com.evayInfo.Inglory.SparkDiary.ml.features

import java.util.Arrays

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.{ChiSqSelector, RFormula, VectorSlicer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by sunlu on 17/6/23.
  * Feature Selectors: VectorSlicer, RFormula, ChiSqSelector
  *
  * 参考链接：https://spark.apache.org/docs/latest/ml-features.html#rformula
  */
object featureSelectorsDemo1 {
  def main(args: Array[String]) {
    /*
    VectorSlicer
     */

    val data = Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val spark = SparkSession.builder.appName("featureSelectorsDemo1").master("local[*]").getOrCreate()
    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))
    import spark.implicits._
    dataset.show()
    /*
+--------------------+
|        userFeatures|
+--------------------+
|(3,[0,1],[-2.0,2.3])|
|      [-2.0,2.3,0.0]|
+--------------------+
     */
    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)
    output.show(false)
    /*
+--------------------+-------------+
|userFeatures        |features     |
+--------------------+-------------+
|(3,[0,1],[-2.0,2.3])|(2,[0],[2.3])|
|[-2.0,2.3,0.0]      |[2.3,0.0]    |
+--------------------+-------------+
     */

    /*
    2. RFormula
     */
    val dataset2 = spark.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")

    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val output2 = formula.fit(dataset2).transform(dataset2)
    output2.select("features", "label").show()


    /*
    3. ChiSqSelector
     */

    val data3 = Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )

    val df3 = spark.createDataset(data3).toDF("id", "features", "clicked")

    val selector = new ChiSqSelector()
      .setNumTopFeatures(1)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")

    val result3 = selector.fit(df3).transform(df3)

    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    result3.show()


    spark.stop()

  }
}
