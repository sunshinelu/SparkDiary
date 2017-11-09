package com.evayInfo.Inglory.LSTM;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.spark.api.TrainingMaster;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/7/13.
 */
public class MnistMLPExample {
    private static final Logger log = LoggerFactory.getLogger(MnistMLPExample.class);

    @Parameter(names = "-useSparkLocal", description = "Use spark local (helper for testing/running without spark submit)", arity = 1)
    //@Parameter这个是参数的意思，参数名是sparklocal，下面的true代表使用local模式，arity=1代表这个参数消费一个参数值
    private boolean useSparkLocal = true;

    @Parameter(names = "-batchSizePerWorker", description = "Number of examples to fit each worker with")
    //每个worker训练多少个例子，下面指定了16
    private int batchSizePerWorker = 16;

    @Parameter(names = "-numEpochs", description = "Number of epochs for training")//训练的步数，下面指定了15
    private int numEpochs = 15;

    public static void main(String[] args) throws Exception {
        new MnistMLPExample().entryPoint(args);//调用入口方法
    }

    protected void entryPoint(String[] args) throws Exception {
        //使用JCommander类处理命令行参数，这个好高端，之前spark也没见过
        JCommander jcmdr = new JCommander(this);
        try {
            jcmdr.parse(args);//解析参数
        } catch (ParameterException e) {
            jcmdr.usage();//如果用户提供无效的输入，提示使用方法
            try {
                Thread.sleep(500);
            } catch (Exception e2) {
            }
            throw e;
        }

        SparkConf sparkConf = new SparkConf();//终于看到了sparkConf
        if (useSparkLocal) {//如果使用local模式，使用所有的核
            sparkConf.setMaster("local[*]");
        }
        sparkConf.setAppName("DL4J Spark MLP Example");//设置任务名
        JavaSparkContext sc = new JavaSparkContext(sparkConf);//创建上下文环境

        //把数据并行载入内存
        //创建继承自BaseDatasetIterator的手写数据迭代器，放入每个worker批大小，是否是训练数据，种子三个参数
        DataSetIterator iterTrain = new MnistDataSetIterator(batchSizePerWorker, true, 12345);

        //同样搞一个测试迭代器,都设置为true的意思是训练和测试使用的是同一批数据
        DataSetIterator iterTest = new MnistDataSetIterator(batchSizePerWorker, true, 12345);
        List<DataSet> trainDataList = new ArrayList<>();//创建训练和测试的数组并把迭代器内容装入数组
        List<DataSet> testDataList = new ArrayList<>();
        while (iterTrain.hasNext()) {
            trainDataList.add(iterTrain.next());
        }
        while (iterTest.hasNext()) {
            testDataList.add(iterTest.next());
        }

        JavaRDD<DataSet> trainData = sc.parallelize(trainDataList);//训练测试数据并行化，变成RDD
        JavaRDD<DataSet> testData = sc.parallelize(testDataList);


        //----------------------------------
        //Create network configuration and conduct network training
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()//下面的和之前一样了
                .seed(12345)
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
                .activation("leakyrelu")
                .weightInit(WeightInit.XAVIER)
                .learningRate(0.02)
                .updater(Updater.NESTEROVS).momentum(0.9)
                .regularization(true).l2(1e-4)
                .list()
                .layer(0, new DenseLayer.Builder().nIn(28 * 28).nOut(500).build())
                .layer(1, new DenseLayer.Builder().nIn(500).nOut(100).build())
                .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
                        .activation("softmax").nIn(100).nOut(10).build())
                .pretrain(false).backprop(true)
                .build();

        //Configuration for Spark training: see http://deeplearning4j.org/spark for explanation of these configuration options
        // 从这个链接可以看spark的相关配置https://deeplearning4j.org/cn/spark
        //Each DataSet object: contains (by default) 32 examples
        // ParameterAveragingTrainingMaster提供了一系列集群运行的配置，上面的链接里有详细说明，建议想用spark的都要通读
        TrainingMaster tm = new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker)
                //该项目控制参数平均化和再分发的频率，按大小等于batchSizePerWorker的微批次的数量计算。总体上的规则是：
                .averagingFrequency(5)
                //平均化周期太短（例如averagingFrequency = 1）可能效率不佳（相对于计算量而言，网络通讯和初始化开销太多）
                //平均化周期太长（例如averagingFrequency = 200）可能会导致表现不佳（不同工作节点实例的参数可能出现很大差异）
                //通常将平均化周期设在5～10 个微批次的范围内比较保险
                //Async prefetching: 2 examples per workerSpark工作节点能够以异步方式预抓取一定数量的微批次（DataSet对象），从而避免数据加载时的等待。
                .workerPrefetchNumBatches(2)
                //将该项的值设置为0会禁用预提取。
                //比较合理的默认值通常是2。过高的值在许多情况下并无帮助（但会使用更多的内存）
                //该项目控制每个工作节点的微批次大小。这与单机定型中的微批次大小设定相仿。换言之，这是每个工作节点中每次参数更新所使用的样例数量
                .batchSizePerWorker(batchSizePerWorker).
                        build();        //Create the Spark network
        //spark配置，网络配置，集群运行配置放入SparkDl4jMultiLayer这个类，获取一个spark网络，感觉已经封装的很高级了
        SparkDl4jMultiLayer sparkNet = new SparkDl4jMultiLayer(sc, conf, tm);
        //Execute training:
        for (int i = 0; i < numEpochs; i++) {//按步数训练，打印步数
            sparkNet.fit(trainData);
            log.info("Completed Epoch {}", i);
        }
        //Perform evaluation (distributed)
        Evaluation evaluation = sparkNet.evaluate(testData);//评估测试数据，并打印
        log.info("***** Evaluation *****");
        log.info(evaluation.stats());
        //Delete the temp training files, now that we are done with them
        tm.deleteTempFiles(sc);//删除临时的训练数据文件
        log.info("***** Example Complete *****");
    }
}
