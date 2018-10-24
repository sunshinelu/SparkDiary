package com.evayInfo.Inglory.Test;


import com.sun.javafx.collections.ListListenerHelper;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringScalarNotEqualStringGroupColumn;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.functions.*;


import java.util.*;

/**
 * 本地测试的    最下面的代码是rdd转Dataset
 * success
 * */
public class jdbcDataTest {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("jdbcDataTest")
                .setMaster("local[2]")
                .set("spark.executor.memory", "2g");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();


        String url = "jdbc:mysql://localhost:3306/data_mining_db?useUnicode=true&characterEncoding=utf8";

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", url);
        options.put("dbtable", "text_similarity_test3");
        options.put("user", "root");
        options.put("password", "123456");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "123456");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");

        /** 所在园区表加载为Dataset*/
        Dataset<Row> originalDS = spark.read().format("jdbc").options(options).load();
        originalDS.show();

        System.out.println("***************");





//        Dataset<Row> stringDS = spark.createDataFrame(list, StringData.class);

//        Dataset<Row> dataDS = originalDS.filter(originalDS.col("var1").contains("我"));
        Dataset<Row> dataDS = originalDS.where(originalDS.col("var1").contains("我"));

        Dataset<Row> dataDS2 = originalDS.toDF().filter(functions.not(originalDS.col("var1").contains("我")));

        Dataset<Row> dataDS3 = originalDS.toDF().withColumn("index",functions.monotonically_increasing_id());

                //jdbcDF.select( "id" , "c3" ).show( false)
                        //（2）selectExpr：可以对指定字段进行特殊处理
                //　　可以直接对指定字段调用UDF函数，或者指定别名等。传入String类型参数，得到DataFrame对象。
                //　　示例，查询id字段，c3字段取别名time，c4字段四舍五入：
                //
                //jdbcDF .selectExpr("id" , "c3 as time" , "round(c4)" ).show(false)
        dataDS.show();

//        szyqRowDS.createOrReplaceTempView("abc");
//        System.out.println("--------------------------------");
//        Dataset<Row> abcDS = spark.sql("update abc set var1 = replace(var1, '100', '21')");


//        szyqRowDS.write().mode("append").jdbc(url, "data_processing_replace_3", connectionProperties);
//        System.out.println("将数据插入到mysql中");

        spark.close();



    }
}

//    /** 主营产品从事该领域的时间 MI_Time1*/
//    Dataset<Row> durationDS = spark.sql("select Jgid, MI_Time1 from fact_cyz_comchampion");
//    JavaPairRDD<String, Integer> id2NumRDD = durationDS.javaRDD().mapToPair(
//            new PairFunction<Row, String, Integer>() {
//                @Override
//                public Tuple2<String, Integer> call(Row row) throws Exception {
//                    String jgid = String.valueOf(row.get(0));
//                    String time = String.valueOf(row.get(1));
//                    String[] timeSplited = time.split("/");
//                    int originalYear = Integer.valueOf(timeSplited[0]);
//                    int thisYear = Integer.valueOf(DateUtils.getThisYear());
//                    int num = thisYear - originalYear;
//                    return new Tuple2<>(jgid, num);
//                }
//            }
//    );
//
//    JavaRDD<Row> id2NumRowRDD = id2NumRDD.map(
//            new Function<Tuple2<String, Integer>, Row>() {
//                @Override
//                public Row call(Tuple2<String, Integer> v1) throws Exception {
//                    return RowFactory.create(v1._1, v1._2);
//                }
//            }
//    );
//
//    List<StructField> structFields = Arrays.asList(
//            DataTypes.createStructField("Jgid", DataTypes.StringType, true),
//            DataTypes.createStructField("num", DataTypes.IntegerType, true));
//    StructType structType = DataTypes.createStructType(structFields);
//
//    Dataset<Row> finalDS = spark.createDataFrame(id2NumRowRDD, structType);
//
//        finalDS.write().mode("overwrite").jdbc(url, "j_cyz_ComChampion_duration", connectionProperties);
//                System.out.println("j_cyz_ComChampion_duration 成功");