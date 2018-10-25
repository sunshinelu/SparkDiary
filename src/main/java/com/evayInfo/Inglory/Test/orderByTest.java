package com.evayInfo.Inglory.Test;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;



/**
 * Created by sunlu on 18/10/24.
 */
public class orderByTest {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("jdbcDataTest")
                .setMaster("local[2]")
                .set("spark.executor.memory", "2g");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String url = "jdbc:mysql://localhost:3306/data_mining_db?useUnicode=true&characterEncoding=utf8";

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", url);
        options.put("dbtable", "data_statistics");
        options.put("user", "root");
        options.put("password", "root");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");

        /** 所在园区表加载为Dataset*/
        Dataset<Row> originalDS = spark.read().format("jdbc").options(options).load();
        originalDS.show();

        System.out.println("***************");

        Dataset<Row> dataDS = originalDS.orderBy(originalDS.col("id").asc(), originalDS.col("var1").desc());
        dataDS.show();
        dataDS.coalesce(1).write().mode("append").jdbc(url, "order_test2", connectionProperties);


        spark.close();



    }
}
