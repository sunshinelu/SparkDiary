package com.evayInfo.Inglory.LSTM;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.BackpropType;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.GravesLSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.spark.api.stats.SparkTrainingStats;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster;
import org.deeplearning4j.spark.stats.EventStats;
import org.deeplearning4j.spark.stats.StatsUtils;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by sunlu on 17/11/8.
 * 参考链接：
 * 深度学习-如何调试基于spark的LSTM：http://blog.csdn.net/chencheng12077/article/details/54692352
 * https://github.com/guluxiaogong/antin-spark/blob/5d981b139506ce1c0565badad8bd4d4c43f8f119/antin-test/src/main/scala/com/zoe/dl/TrainingStatsExample.java
 */
public class TrainingStatsExample {
    private static final Logger log = LoggerFactory.getLogger(TrainingStatsExample.class);

    @Parameter(names = "-useSparkLocal", description = "Use spark local (helper for testing/running without spark submit)", arity = 1)
//设置参数名，描述，使用参数数量
    private boolean useSparkLocal = true;//设置参数值，使用本地模式

    public static void main(String[] args) throws Exception {
        new TrainingStatsExample().entryPoint(args);//传入参数，调用入口函数
    }

    private void entryPoint(String[] args) throws Exception {
        //Handle command line arguments
        JCommander jcmdr = new JCommander(this);//处理命令行的类
        try {
            jcmdr.parse(args);//解析
        } catch (ParameterException e) {
            //User provides invalid input -> print the usage info
            jcmdr.usage();//如果是无效输入，打印错误
            try {
                Thread.sleep(500);
            } catch (Exception e2) {
            }
            throw e;
        }


        //Set up network configuration:
        MultiLayerConfiguration config = getConfiguration();//获取配置，可以先看后面的函数

        //Set up the Spark-specific configuration
        int examplesPerWorker = 8;      //i.e., minibatch size that each worker gets//配置spark参数，每个工作节点每次参数更新的批次大小
        int averagingFrequency = 3;     //Frequency with which parameters are averaged//参数平均化的频率，3次

        //Set up Spark configuration and context
        SparkConf sparkConf = new SparkConf();//使用spark本地模式运行
        if (useSparkLocal) {
            sparkConf.setMaster("local[*]");
            log.info("Using Spark Local");
        }
        sparkConf.setAppName("DL4J Spark Stats Example");//设置spark任务描述
        JavaSparkContext sc = new JavaSparkContext(sparkConf);//spark上下文环境

        //Get data. See SparkLSTMCharacterExample for details
        JavaRDD<DataSet> trainingData = SparkLSTMCharacterExample.getTrainingData(sc);//使用SparkLSTMCharacterExample的获取数据方法，这个下一篇也会详细讲


        //Set up the TrainingMaster. The TrainingMaster controls how learning is actually executed on Spark
        //Here, we are using standard parameter averaging
        int examplesPerDataSetObject = 1;   //We haven't pre-batched our data: therefore each DataSet object contains 1 example
        ParameterAveragingTrainingMaster tm = new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)
                .workerPrefetchNumBatches(2)    //Async prefetch 2 batches for each worker
                .averagingFrequency(averagingFrequency)
                .batchSizePerWorker(examplesPerWorker)
                .build();//设置TrainingMaster,ParameterAveragingTrainingMaster定义了一系列配置选项，用于控制定型的执行方式，包括每个样本代表一个数据集对象，每个worker异步获取2个批次数据，每3个批次进行参数平均化，worker处理的批大小是8

        //Create the Spark network
        SparkDl4jMultiLayer sparkNetwork = new SparkDl4jMultiLayer(sc, config, tm);//构建spark的multilayernetworker，即SparkDl4jMultiLayer
        /////////iLayerNetwork

        //*** Tell the network to collect training statistics. These will NOT be collected by default ***
        sparkNetwork.setCollectTrainingStats(true);//设置收集训练的统计信息，默认是不收集的

        //Fit for 1 epoch:
        sparkNetwork.fit(trainingData);//训练一步

        //Delete the temp training files, now that we are done with them (if fitting for multiple epochs: would be re-used)
        tm.deleteTempFiles(sc);//删除临时训练文件，如果训练多步会重用这份数据，这里为什么会产生这个文件？看了官网，大概是由于使用导出式的定型方法，先将RDD<DataSet>将以分批次和序列化的形式保存至磁盘，执行器随后会按要求异步加载DataSet对象，也就是说删除的是分批次和序列化的形式保存的副本

        //Get the statistics:
        SparkTrainingStats stats = sparkNetwork.getSparkTrainingStats();//获取训练统计信息
        Set<String> statsKeySet = stats.getKeySet();    //Keys for the types of statistics//获取统计信息项
        System.out.println("--- Collected Statistics ---");
        for (String s : statsKeySet) {
            System.out.println(s);//遍历统计项
        }

        //Demo purposes: get one statistic and print it
        String first = statsKeySet.iterator().next();//把统计项变成迭代器，取出一个
        List<EventStats> firstStatEvents = stats.getValue(first);//获取对应的值，结果是一个list
        EventStats es = firstStatEvents.get(0);//获取列表的第一个值
        log.info("Training stats example:");//打印机器id,jvmid,线程id,开始时间，消耗时间
        log.info("Machine ID:     " + es.getMachineID());
        log.info("JVM ID:         " + es.getJvmID());
        log.info("Thread ID:      " + es.getThreadID());
        log.info("Start time ms:  " + es.getStartTime());
        log.info("Duration ms:    " + es.getDurationMs());

        //Export a HTML file containing charts of the various stats calculated during training
        StatsUtils.exportStatsAsHtml(stats, "SparkStats.html", sc);//导出html文件，包含各统计信息的表
        log.info("Training stats exported to {}", new File("SparkStats.html").getAbsolutePath());//打印文件名

        log.info("****************Example finished********************");
    }


    //Configuration for the network we will be training
    private static MultiLayerConfiguration getConfiguration() {
        int lstmLayerSize = 200;               //Number of units in each GravesLSTM layer//每个LSTM层的节点数
        int tbpttLength = 50;                       //Length for truncated backpropagation through time. i.e., do parameter updates ever 50 characters截断式bptt中网络能够学习的依赖长度

        Map<Character, Integer> CHAR_TO_INT = SparkLSTMCharacterExample.getCharToInt();//调用SparkLSTMCharacterExample中的getCharToInt()方法，这个类我们在下一篇介绍，该方法把输入字符数组解析成唯一字符和出现次数的map


        int nIn = CHAR_TO_INT.size();//把解析出的map大小赋值给输入和输出大小
        int nOut = CHAR_TO_INT.size();

        //Set up network configuration:
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()//下面的配置还是老样子
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
                .learningRate(0.1)
                .updater(Updater.RMSPROP).rmsDecay(0.95)
                .seed(12345)
                .regularization(true).l2(0.001)
                .weightInit(WeightInit.XAVIER)
                .list()
                .layer(0, new GravesLSTM.Builder().nIn(nIn).nOut(lstmLayerSize).activation("tanh").build())//第一层是LSTM层输入是唯一字符数输出是200，我发现LSTM首层的输出总是要大于输入
                .layer(1, new GravesLSTM.Builder().nIn(lstmLayerSize).nOut(lstmLayerSize).activation("tanh").build())//第二层也是LSTM层，输入是上层的输出且输入和输出相等
                .layer(2, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MCXENT).activation("softmax")        //MCXENT + softmax for classification//输出层是RNN层，由于是多类分类，用softmax作为激活函数，输入层不变，输出层还原到最开始输入的大小
                        .nIn(lstmLayerSize).nOut(nOut).build())
                .backpropType(BackpropType.TruncatedBPTT).tBPTTForwardLength(tbpttLength).tBPTTBackwardLength(tbpttLength)//使用截断式bptt，每次参数更新的正反向长度都是50
                .pretrain(false).backprop(true)
                .build();

        return conf;//返回设置
    }
}