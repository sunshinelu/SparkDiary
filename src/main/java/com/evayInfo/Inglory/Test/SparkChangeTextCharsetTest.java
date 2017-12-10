package com.evayInfo.Inglory.Test;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by sunlu on 17/12/8.
 * 参考链接：
 * https://www.cnblogs.com/xiaodf/p/5027176.html
 * <p/>
 * 利用Spark框架读取HDFS文件，文件格式为GB2312，转换为UTF-8，实现WordCount示例
 */
public class SparkChangeTextCharsetTest {
    public static void main(String[] args) {




        String str1 = new String(" 中国人");
        try {
            byte[] strby = str1.getBytes("GB2312");
            String Str2 = new String(strby,"utf-8");
            System.out.println(Str2);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /*
    private static final Pattern SPACE = Pattern.compile(",");

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }
        String inputSparkFile = args[0];
        String outputSparkFile = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("SparkWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        Configuration conf = new Configuration();
        JavaPairRDD<LongWritable, Text> contents = ctx.newAPIHadoopFile(
                inputSparkFile, TextInputFormat.class, LongWritable.class,
                Text.class, conf);
        JavaRDD<String> lines = contents
                .map(new Function<Tuple2<LongWritable, Text>, String>() {

                    public String call(Tuple2<LongWritable, Text> x) {
                        String lines = null;
                        try {
                            lines = new String(x._2().getBytes(), 0, x._2()
                                    .getLength(), "GB2312");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                        return lines;
                    }
                });
        // JavaRDD<String> changeLines = lines
        // .filter(new Function<String, Boolean>() {
        // public Boolean call(String s) {
        // return s.contains("234");
        // }
        // });
        JavaRDD<String> words = lines
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) {
                        return Arrays.asList(SPACE.split(s));
                    }
                });

        JavaPairRDD<String, Integer> ones = words
                .mapToPair(new PairFunction<String, String, Integer>() {

                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = ones
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        counts.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> arg0) throws Exception {
                return arg0._1.toUpperCase() + ": " + arg0._2;
            }
        }).saveAsTextFile(outputSparkFile);

        ctx.stop();
    }
    */
}
