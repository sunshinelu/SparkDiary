package com.evayInfo.Inglory.SparkDiary.ml.classification;

import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;//不是mllib
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by sunlu on 17/12/26.
 * 参考链接：
 * PCA主成份分析（Spark 2.0）
 * http://blog.csdn.net/qq_34531825/article/details/52347220
 */
public class myPCA {
    public static void main(String[] args) {
        SparkSession spark=SparkSession
                .builder()
                .appName("myLR")
                .master("local[4]")
                .getOrCreate();
        Dataset<Row> rawDataFrame=spark.read().format("libsvm")
                .load("/home/hadoop/spark/spark-2.0.0-bin-hadoop2.6" +
                        "/data/mllib/sample_libsvm_data.txt");
        //首先对特征向量进行标准化
        Dataset<Row> scaledDataFrame=new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setWithMean(false)//对于稀疏数据（如本次使用的数据），不要使用平均值
                .setWithStd(true)
                .fit(rawDataFrame)
                .transform(rawDataFrame);
        //PCA Model
        PCAModel pcaModel=new PCA()
                .setInputCol("scaledFeatures")
                .setOutputCol("pcaFeatures")
                .setK(3)//
                .fit(scaledDataFrame);
        //进行PCA降维
        pcaModel.transform(scaledDataFrame).select("label","pcaFeatures").show(100,false);
        /*
        //PCA Model 参数选择
        PCAModel pcaModel=new PCA()
                      .setInputCol("scaledFeatures")
                      .setOutputCol("pcaFeatures")
                      .setK(100)//
                      .fit(scaledDataFrame);
        int i=1;
        for(double x:pcaModel.explainedVariance().toArray()){
        System.out.println(i+"\t"+x+"  ");
        i++;
        }
         */
    }
}
