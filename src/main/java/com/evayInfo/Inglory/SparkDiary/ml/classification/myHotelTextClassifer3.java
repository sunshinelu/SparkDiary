package com.evayInfo.Inglory.SparkDiary.ml.classification;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.io.StringReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

//引用IKAnalyzer2012的类
import org.wltea.analyzer.lucene.IKAnalyzer;

import com.evayInfo.Inglory.SparkDiary.ml.classification.LabelValue;

//文本处理，酒店评论
/**
 * Created by sunlu on 17/12/26.
 * 参考链接：
 Spark2.0 特征提取、转换、选择之二：特征选择、文本处理，以中文自然语言处理(情感分类为例)
 http://blog.csdn.net/qq_34531825/article/details/52431264
 *
 */

public class myHotelTextClassifer3 {
    public static void main(String[] args) throws IOException {

        SparkSession spark=SparkSession
                .builder()
                .appName("Chinese Text Processing")
                .master("local[4]")
                .config("spark.sql.warehouse.dir",
                        "file///:G:/Projects/Java/Spark/spark-warehouse" )
                .getOrCreate();

        //屏蔽日志
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
        //--------------------------(0)读入数据，数据预处理--------------------------------
        //原始数据文件包含两行，一行是label，一行是sentence，csv格式的
        Dataset<Row> raw=spark.read().format("csv")
                .load("E:/data/utf/ChnSentiCorp_htl_ba_2000/all.csv");

        //去掉空值，不然一定后面一定抛出nullpointer异常
        //distinct数据去重 ,并打乱数据的顺序：不然数据是先负样本后正样本有规律排列的，预测效果偏高
        Dataset<Row> rawFilterNaN=raw
                .filter(raw.col("_c0").isNotNull())
                .filter(raw.col("_c1").isNotNull())
                .distinct();

        //--------------------------(1)分词----------------------------------------
        //为Map自定义了Class LabelValue，见后面
        Encoder<LabelValue> LongStringEncoder=Encoders.bean(LabelValue.class);
        Dataset<LabelValue> wordsDF=rawFilterNaN.map(new MapFunction<Row,LabelValue>() {
            @Override
            public LabelValue call(Row row) throws Exception {
                if (row!=null) {
                    LabelValue ret = new LabelValue();
                    double Label=1.0;
                    if (row.getString(0).equals("0.0")) {
                        Label=0.0;
                    }else{
                        Label=1.0;
                    }
                    ret.setLabel(Label);

                    //-------------KAnalyzer分词--------------------
                    //创建分词对象
                    Analyzer anal=new IKAnalyzer(true);
                    StringReader reader=new StringReader(row.getString(1));
                    //分词
                    TokenStream ts=anal.tokenStream("", reader);
                    CharTermAttribute term=(CharTermAttribute) ts
                            .getAttribute(CharTermAttribute.class);
                    //遍历分词数据
                    String words="";
                    while(ts.incrementToken()){
                        words+=(term.toString()+"|");
                    }
                    ret.setValue(words);
                    reader.close();

                    return ret;
                }
                else {
                    return null;
                }
            }

        }, LongStringEncoder);


        //--------------------------(1)-2 RegexTokenizer分词器-----------------------------
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("value")
                .setOutputCol("words")
                .setPattern("\\|");

        Dataset<Row> wordsDF2 = regexTokenizer.transform(wordsDF);


        //--------------------------(2) HashingTF训练词频矩阵---------------------------------

        HashingTF tf=new HashingTF()
                .setInputCol("words")
                .setOutputCol("TF");
        Dataset<Row> wordsTF=tf.transform(wordsDF2).select("TF","label");
        wordsTF.show();wordsTF.printSchema();
        Dataset<Row> wordsTF2=wordsTF
                .filter(wordsTF.col("TF").isNotNull())
                .filter(wordsTF.col("label").isNotNull());


        //------------------------- (4)计算 TF-IDF 矩阵--------------------------------------
        IDFModel idf=new IDF()
                .setInputCol("TF")
                .setOutputCol("features")
                .fit(wordsTF2);
        Dataset<Row> wordsTfidf=idf.transform(wordsTF2);


        //----------------------------(5)NaiveBayesModel ML---------------------
        Dataset<Row>[] split=wordsTfidf.randomSplit(new double[]{0.6,0.4});
        Dataset<Row> training=split[0];
        Dataset<Row> test=split[1];

        NaiveBayes naiveBayes=new NaiveBayes()
                .setLabelCol("label")
                .setFeaturesCol("features");
        NaiveBayesModel naiveBayesModel=naiveBayes.fit(training);

        Dataset<Row> predictDF=naiveBayesModel.transform(test);

        //自定义计算accuracy
        double total=(double) predictDF.count();
        Encoder<Double> doubleEncoder=Encoders.DOUBLE();

        Dataset<Double> accuracyDF=predictDF.map(new MapFunction<Row,Double>() {
            @Override
            public Double call(Row row) throws Exception {
                if((double)row.get(1)==(double)row.get(5)){return 1.0;}
                else {return 0.0;}
            }
        }, doubleEncoder);

        accuracyDF.createOrReplaceTempView("view");
        double correct=(double) spark.sql("SELECT value FROM view WHERE value=1.0").count();
        System.out.println("accuracy "+(correct/total));

        //计算areaUnderRoc
        double areaUnderRoc=new BinaryClassificationEvaluator()
                .setLabelCol("label")
                .setRawPredictionCol("prediction")
                .evaluate(predictDF);
        //(areaUnderROC|areaUnderPR) (default: areaUnderROC)
        System.out.println("areaUnderRoc "+areaUnderRoc);
        /*
        //转换为词向量，并进行标准化
Word2Vec word2Vec=new Word2Vec()
                .setInputCol("words")
                .setOutputCol("vect")
                .setVectorSize(10);
        Dataset<Row> vect=word2Vec
                .fit(wordsDF2)
                .transform(wordsDF2);
        //vect.show();vect.printSchema();
        //正则化
        Dataset<Row> vect2=new MinMaxScaler()
                .setInputCol("vect")
                .setOutputCol("features")
                .setMax(1.0)
                .setMin(0.0)
                .fit(vect)
                .transform(vect);
        //vect2.show();vect2.printSchema();

//GBTC分类
GBTClassifier gbtc=new GBTClassifier()
                .setLabelCol("label")
                .setFeaturesCol("vect")
                .setMaxDepth(10)
                .setMaxIter(10)
                .setStepSize(0.1);
        Dataset<Row> predictDF=gbtc.fit(training0).transform(test0);
 //其余代码是一样的，可以尝试不同的参数组合。
         */
    }
}



//结果分析
//accuracy 0.7957860615883307
//areaUnderRoc 0.7873761854583772
//应该还有提升的空间



