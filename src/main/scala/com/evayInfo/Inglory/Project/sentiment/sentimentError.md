## 1. 报错：Schema for type Char is not supported

Exception in thread "main" java.lang.UnsupportedOperationException: Schema for type Char is not supported
	at org.apache.spark.sql.catalyst.ScalaReflection$.schemaFor(ScalaReflection.scala:733)
	at org.apache.spark.sql.catalyst.ScalaReflection$.schemaFor(ScalaReflection.scala:671)
	at org.apache.spark.sql.functions$.udf(functions.scala:3072)
	at com.evayInfo.Inglory.Project.sentiment.dataClean.dc_weixin$.main(dc_weixin.scala:68)
	at com.evayInfo.Inglory.Project.sentiment.dataClean.dc_weixin.main(dc_weixin.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:497)
	at com.intellij.rt.execution.application.AppMain.main(AppMain.java:140)

将

     val addSource2 = udf("WEIXIIN")
     val df3 = df1.withColumn("Source", addSource2())
     df3.printSchema()

改为：

    val addSource = udf((arg:String) => "WEIXIIN")
    val df2 = df1.withColumn("Source", addSource($"WX_ID"))
    df2.printSchema()
    
则错误消失。

原因尚不明确。

## 2. cannot resolve '`col1`' given input columns: [_1, _2, _3];

Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Exception in thread "main" org.apache.spark.sql.AnalysisException: cannot resolve '`col1`' given input columns: [_1, _2, _3];
	at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:77)
	at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:74)
	at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:310)
	at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:310)
....
....
....
	at com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean.colRename$.main(colRename.scala:27)
	at com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean.colRename.main(colRename.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:497)
	at com.intellij.rt.execution.application.AppMain.main(AppMain.java:140)

代码为：

    package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean
    
    import org.apache.log4j.{Level, Logger}
    import org.apache.spark.sql.SparkSession
    
    /**
     * Created by sunlu on 17/7/19.
     */
    object colRename {
    
      def SetLogger = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("com").setLevel(Level.OFF)
        System.setProperty("spark.ui.showConsoleProgress", "false")
        Logger.getRootLogger().setLevel(Level.OFF);
      }
    
      case class colName1(col1: Int, col2: String, col3: String)
      def main(args: Array[String]) {
        SetLogger
        val spark = SparkSession.builder.appName("colRename").master("local[*]").getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._
    
        val rdd = sc.parallelize(Seq((1, "a", "c"), (2, "d", "f"), (3, "r", "e")))
    
        val ds1 = spark.createDataFrame(rdd).as[colName1]
        ds1.printSchema()
    
        val ds1ColumnsName = Seq("id", "s1", "s2")
        val ds2 = ds1.as[(Int, String,String)].toDF(ds1ColumnsName: _*)
        ds2.printSchema()
    
        val df1 = spark.createDataFrame(rdd).as[colName1]
        df1.printSchema()
    
        val df2 = df1.toDF(ds1ColumnsName: _*)
        df2.printSchema()
    
    
      }
    
    }


改为：

    package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean
    
    import org.apache.log4j.{Level, Logger}
    import org.apache.spark.sql.SparkSession
    
    /**
     * Created by sunlu on 17/7/19.
     */
    object colRename {
    
      def SetLogger = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("com").setLevel(Level.OFF)
        System.setProperty("spark.ui.showConsoleProgress", "false")
        Logger.getRootLogger().setLevel(Level.OFF);
      }
    
      case class colName1(col1: Int, col2: String, col3: String)
      def main(args: Array[String]) {
        SetLogger
        val spark = SparkSession.builder.appName("colRename").master("local[*]").getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._
    
        val rdd = sc.parallelize(Seq(colName1(1, "a", "c"), colName1(2, "d", "f"), colName1(3, "r", "e")))
    
        val ds1 = spark.createDataFrame(rdd)//.as[colName1]
        ds1.printSchema()
    
        val ds1ColumnsName = Seq("id", "s1", "s2")
        val ds2 = ds1.as[(Int, String,String)].toDF(ds1ColumnsName: _*)
        ds2.printSchema()
    
        val df1 = spark.createDataFrame(rdd)//.as[colName1]
        df1.printSchema()
    
        val df2 = df1.toDF(ds1ColumnsName: _*)
        df2.printSchema()
    
    
      }
    
    }

运行正常。

`as`的使用方法需要好好调研一下。
