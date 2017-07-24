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

## 3. Failed to execute user defined function(anonfun$8: (string) => string)

Caused by: org.apache.spark.SparkException: Failed to execute user defined function(anonfun$8: (string) => string)

      /*
    getWeiboData：获取清洗后的微博数据全部数据
    */
      def getWeiboData(spark: SparkSession, url: String, user: String, password: String,
                       wTable: String, wCommentTable: String): DataFrame = {
        // get DA_WEIBO
        val df_w = mysqlUtil.getMysqlData(spark, url, user, password, wTable)
        // get DA_WEIBO_COMMENTS
        val df_c = mysqlUtil.getMysqlData(spark, url, user, password, wCommentTable)
        // select columns
        val df_w_1 = df_w.select("ID", "TITLE", "TEXT", "CREATEDAT", "WEIBO_KEY").withColumn("WEIBO_ID", col("ID"))
        val df_c_1 = df_c.select("ID", "WEIBO_ID", "TEXT", "CREATED_AT")
        // 通过`WEIBO_ID`从`DA_WEIBO`表中`WEIBO_KEY`列获取。
        val keyLib = df_w_1.select("WEIBO_ID", "TITLE", "WEIBO_KEY")
        val df_c_2 = df_c_1.join(keyLib, Seq("WEIBO_ID"), "left").select("ID", "TITLE", "TEXT", "CREATED_AT", "WEIBO_KEY")
        val df_w_2 = df_w_1.drop("WEIBO_ID")
    
        // add IS_COMMENT column
        val addIsComm = udf((arg: Int) => arg)
        val df_w_3 = df_w_2.withColumn("IS_COMMENT", addIsComm(lit(0)))
        val df_c_3 = df_c_2.withColumn("IS_COMMENT", lit(1))
    
        // change all columns name
        val colRenamed = Seq("ARTICLEID", "TITLE", "TEXT", "TIME", "KEYWORD", "IS_COMMENT")
        val df_w_4 = df_w_3.toDF(colRenamed: _*)
        val df_c_4 = df_c_3.toDF(colRenamed: _*)
    
        // 合并 df_w_4 和 df_c_4
        val df = df_w_4.union(df_c_4)
    
        // add source column
        val addSource = udf((arg: String) => "WEIBO")
        val df1 = df.withColumn("SOURCE", addSource(col("ARTICLEID"))).
          na.drop(Array("TEXT")).filter(length(col("TEXT")) >= 15)
    
        //使用Jsoup进行字符串处理
        val jsoupExtFunc = udf((content: String) => {
          Jsoup.parse(content).body().text()
        })
        val df2 = df1.withColumn("JsoupExt", jsoupExtFunc(col("TEXT")))
        //df2.select("JsoupExt").take(5).foreach(println)
    
        // 表情符号的替换
        val emoticonPatten = "\\[[0-9a-zA-Z\\u4e00-\\u9fa5]+\\]".r
        val rmEmtionFunc = udf((arg: String) => {
          emoticonPatten.replaceAllIn(arg, "").mkString("")
        })
        val df3 = df2.withColumn("TEXT_pre", rmEmtionFunc(col("JsoupExt"))).drop("JsoupExt")
    
        // 提取微博中的正文，并添加系统时间列
        val contentPatten = "//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+[\\u4e00-\\u9fa5a-zA-Z0-9_：【】,.?:;'\"!，。！“”；？]+|@[^,，：:\\s@]+|#[^#]+#".r
        val getContentFunc = udf((arg: String) => {
          contentPatten.replaceAllIn(arg, "").mkString("")
        })
        val df4 = df3.withColumn("CONTENT", getContentFunc(col("TEXT_pre"))).drop("TEXT_pre").na.drop(Array("CONTENT")).filter(length(col("CONTENT")) >= 1)
        df4
      }

使用上述代码进行数据清洗时，会报错`Caused by: org.apache.spark.SparkException: Failed to execute user defined function(anonfun$8: (string) => string)`

将该行代码：

    val df4 = df3.withColumn("CONTENT", getContentFunc(col("TEXT_pre"))).drop("TEXT_pre").na.drop(Array("CONTENT")).filter(length(col("CONTENT")) >= 1)

修改为：

    val df4 = df3.withColumn("CONTENT", getContentFunc(col("TEXT_pre"))).drop("TEXT_pre").na.drop(Array("CONTENT"))
    
后，代码不报错。原因上不明确。