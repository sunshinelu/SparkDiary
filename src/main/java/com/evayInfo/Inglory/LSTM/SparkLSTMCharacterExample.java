package com.evayInfo.Inglory.LSTM;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.BackpropType;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.GravesLSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.api.IterationListener;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;

/**
 * Created by sunlu on 17/11/8.
 * 参考链接：
 * 深度学习-基于spark的LSTM：http://blog.csdn.net/chencheng12077/article/details/54707809
 * https://github.com/guluxiaogong/antin-spark/blob/5d981b139506ce1c0565badad8bd4d4c43f8f119/antin-test/src/main/scala/com/zoe/dl/SparkLSTMCharacterExample.java
 *
 */
public class SparkLSTMCharacterExample {
    private static final Logger log = LoggerFactory.getLogger(SparkLSTMCharacterExample.class);

    private static Map<Integer, Character> INT_TO_CHAR = getIntToChar();//调用函数，返回索引和对应字符的map,可以先看后面的函数
    private static Map<Character, Integer> CHAR_TO_INT = getCharToInt();//调用函数，返回字符和对应索引的map，可以先看后面的函数
    private static final int N_CHARS = INT_TO_CHAR.size();//计算索引数
    private static int nOut = CHAR_TO_INT.size();//计算字符数
    private static int exampleLength = 1000;//Length of each training example sequence to use//训练实例序列的长度为100

    @Parameter(names = "-useSparkLocal", description = "Use spark local (helper for testing/running without spark submit)", arity = 1)
    //各种参数不再赘述了
    private boolean useSparkLocal = true;

    @Parameter(names = "-batchSizePerWorker", description = "Number of examples to fit each worker with")
    private int batchSizePerWorker = 8;   //How many examples should be used per worker (executor) when fitting?

    @Parameter(names = "-numEpochs", description = "Number of epochs for training")
    private int numEpochs = 1;

    public static void main(String[] args) throws Exception {
        new SparkLSTMCharacterExample().entryPoint(args);//调用入口函数
    }

    protected void entryPoint(String[] args) throws Exception {
        //Handle command line arguments
        JCommander jcmdr = new JCommander(this);//jCommander处理参数也不说了
        try {
            jcmdr.parse(args);
        } catch (ParameterException e) {
            //User provides invalid input -> print the usage info
            jcmdr.usage();
            try {
                Thread.sleep(500);
            } catch (Exception e2) {
            }
            throw e;
        }

        Random rng = new Random(12345);//随机生成器
        //Number of units in each GravesLSTM layer
        // LSTM层节点数量
        int lstmLayerSize = 200;
        //Length for truncated backpropagation through time. i.e., do parameter updates ever 50 characters
        // 截断式bptt中网络学习的长度
        int tbpttLength = 50;
        //Number of samples to generate after each training epoch
        // 每个训练步后生成的例子数量，这是要模仿写文章，所以有这个参数
        int nSamplesToGenerate = 4;
        //Length of each sample to generate
        // 生成例子的长度300，这是要模仿写文章，所以有这个参数
        int nCharactersToSample = 300;
        //Optional character initialization; a random character is used if null
        // 初始化字符，这里是随机字符
        String generationInitialization = null;
        // Above is Used to 'prime' the LSTM with a character sequence to continue/complete.
        // Initialization characters must all be in CharacterIterator.getMinimalCharacterSet() by default

        //Set up network configuration://设置网络
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
                .learningRate(0.1)
                .rmsDecay(0.95)
                .seed(12345)
                .regularization(true)
                .l2(0.001)
                .weightInit(WeightInit.XAVIER)
                .updater(Updater.RMSPROP)
                .list()
                        //第一层是LSTM，输入大小独立字符数，输出大小是200，果然又是放大了好多，可见cnn是把节点越搞越小，rnn是把节点越搞越大
                .layer(0, new GravesLSTM.Builder().nIn(CHAR_TO_INT.size()).nOut(lstmLayerSize)
                        .activation("tanh").build())
                        //第二层还是LSTM层，输入输出节点都是200
                .layer(1, new GravesLSTM.Builder().nIn(lstmLayerSize).nOut(lstmLayerSize)
                        .activation("tanh").build())
                        //MCXENT + softmax for classification
                        // 输出层是RNN,由于是分类采用softmax作为激活函数,输入大小是200，输出和原始输入大小一致
                .layer(2, new RnnOutputLayer.Builder(LossFunction.MCXENT).activation("softmax")
                        .nIn(lstmLayerSize).nOut(nOut).build())
                        //使用截断式bptt，截断长度为50，即正反向参数更新参考的长度都是50
                .backpropType(BackpropType.TruncatedBPTT).tBPTTForwardLength(tbpttLength).tBPTTBackwardLength(tbpttLength)
                .pretrain(false).backprop(true)
                .build();


        //-------------------------------------------------------------
        //Set up the Spark-specific configuration//配置spark
        /* How frequently should we average parameters (in number of minibatches)?
        Averaging too frequently can be slow (synchronization + serialization costs) whereas too infrequently can result
        learning difficulties (i.e., network may not converge) */
        int averagingFrequency = 3;//参数平均化的频率，3批平均一次

        //Set up Spark configuration and context
        SparkConf sparkConf = new SparkConf();//使用spark本地模式
        if (useSparkLocal) {
            sparkConf.setMaster("local[*]");
        }
        sparkConf.setAppName("LSTM Character Example");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //获取数据得到训练RDD，跳到这个函数
        JavaRDD<DataSet> trainingData = getTrainingData(sc);


        //Set up the TrainingMaster. The TrainingMaster controls how learning is actually executed on Spark
        //Here, we are using standard parameter averaging
        //For details on these configuration options, see: https://deeplearning4j.org/spark#configuring//设置tm
        int examplesPerDataSetObject = 1;//每个DataSet对象有一个例子
        ParameterAveragingTrainingMaster tm = new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)//构建tm
                .workerPrefetchNumBatches(2)    //Asynchronously prefetch up to 2 batches//异步获取2批数据
                .averagingFrequency(averagingFrequency)//参数平均化的频率是3
                .batchSizePerWorker(batchSizePerWorker)//每个worker处理批的大小是8
                .build();
        //把参数传入spark的网络配置
        SparkDl4jMultiLayer sparkNetwork = new SparkDl4jMultiLayer(sc, conf, tm);
        //设置监听器,singletonList返回一个包含具体对象的不可变list
        sparkNetwork.setListeners(Collections.<IterationListener>singletonList(new ScoreIterationListener(1)));


        //Do training, and then generate and print samples from network
        for (int i = 0; i < numEpochs; i++) {//按步数训练，生成并打印新写的例子，每步最后返回一个训练网络的副本
            //Perform one epoch of training. At the end of each epoch, we are returned a copy of the trained network
            MultiLayerNetwork net = sparkNetwork.fit(trainingData);//定型网络

            //Sample some characters from the network (done locally)//本地随机化一些字符
            log.info("Sampling characters from network given initialization \"" +
                    (generationInitialization == null ? "" : generationInitialization) + "\"");
            String[] samples = sampleCharactersFromNetwork(generationInitialization, net, rng, INT_TO_CHAR,
                    nCharactersToSample, nSamplesToGenerate);//利用学习的生成新的例子，看下这个函数
            for (int j = 0; j < samples.length; j++) {//打印随机化字符
                log.info("----- Sample " + j + " -----");
                log.info(samples[j]);
            }
        }

        //Delete the temp training files, now that we are done with them
        tm.deleteTempFiles(sc);//删除临时文件

        log.info("\n\nExample complete");
    }


    /**
     * Get the training data - a JavaRDD<DataSet>//注释说这个获取数据的方法是字符建模的特例，不是最佳实践
     * Note that this approach for getting training data is a special case for this example (modelling characters), and
     * should  not be taken as best practice for loading data (like CSV etc) in general.
     */
    public static JavaRDD<DataSet> getTrainingData(JavaSparkContext sc) throws IOException {
        //Get data. For the sake of this example, we are doing the following operations:
        // File -> String -> List<String> (split into length "sequenceLength" characters) -> JavaRDD<String> -> JavaRDD<DataSet>//为了获取文件，我们从文件到字符串到序列长度的list再到RDD最终到DataSet的RDD
        List<String> list = getShakespeareAsList(exampleLength);//获取长度为1000的字符串list
        JavaRDD<String> rawStrings = sc.parallelize(list);//并行化数据
        Broadcast<Map<Character, Integer>> bcCharToInt = sc.broadcast(CHAR_TO_INT);//广播字符和索引的map
        return rawStrings.map(new StringToDataSetFn(bcCharToInt));//又见scala的map,把并行化的数据
    }


    private static class StringToDataSetFn implements Function<String, DataSet> {//java做map就得实现一个函数，这样功能就类似于scala的map了,Function的第一个参数是输入类型字符串，第二个参数是结果类型DataSet
        private final Broadcast<Map<Character, Integer>> ctiBroadcast;//定义一个广播变量

        private StringToDataSetFn(Broadcast<Map<Character, Integer>> characterIntegerMap) {//构造函数，接收传入的广播变量
            this.ctiBroadcast = characterIntegerMap;
        }

        @Override
        public DataSet call(String s) throws Exception {//回调函数，返回DataSet
            //Here: take a String, and map the characters to a one-hot representation//把字符串搞成one-hot描述
            Map<Character, Integer> cti = ctiBroadcast.getValue();//广播变量的内容
            int length = s.length();//字符串长度，由于最后一个长度可能不是1000，所以求下长度
            INDArray features = Nd4j.zeros(1, N_CHARS, length - 1);//从spark的数据弄成nd4j的数据，第一个参数代表有1个元素，第二个参数代表这个矩阵元素的行即字符索引数，第三个参数代表这个矩阵元素的列即字符的长度
            INDArray labels = Nd4j.zeros(1, N_CHARS, length - 1);//同理再搞一个放标签
            char[] chars = s.toCharArray();//把字符串转成字符数组
            int[] f = new int[3];//搞两个长度为3的整形数组
            int[] l = new int[3];
            for (int i = 0; i < chars.length - 2; i++) {//遍历字符数组
                f[1] = cti.get(chars[i]);//在广播变量里搜索字符的索引，放到f的第二个位置，把的字符数组索引放入f的第三个位置
                f[2] = i;
                l[1] = cti.get(chars[i + 1]);   //Predict the next character given past and current characters
                l[2] = i;//在广播变量里搜索下一个字符的索引，放到l的第二个字符，把字符数组索引放入l的第三个位置，有点预测下一个字符的意思

                features.putScalar(f, 1.0);//这里看出f第一个位置不放数字的原因是nd4j高维数组只有1个元素，f代表位置索引，1代表把f代表的位置置为1，one-hot一般都是这个套路
                labels.putScalar(l, 1.0);//同理把标签放好
            }
            return new DataSet(features, labels);//DataSet装入特征和标签，这也是单行map计算的返回结果
        }
    }

    //This function downloads (if necessary), loads and splits the raw text data into "sequenceLength" strings
    private static List<String> getShakespeareAsList(int sequenceLength) throws IOException {//下载数据并切分长度为1000的字符串列表
        //The Complete Works of William Shakespeare//数据概要，莎士比亚
        //5.3MB file in UTF-8 Encoding, ~5.4 million characters
        //https://www.gutenberg.org/ebooks/100
        String url = "https://s3.amazonaws.com/dl4j-distribution/pg100.txt";//从哪下
        String tempDir = System.getProperty("java.io.tmpdir");//下载目录
        String fileLocation = tempDir + "/Shakespeare.txt";    //Storage location from downloaded file//下载文件名
        File f = new File(fileLocation);//声明文件类
        if (!f.exists()) {//不存在就下载
            FileUtils.copyURLToFile(new URL(url), f);
            System.out.println("File downloaded to " + f.getAbsolutePath());
        } else {
            System.out.println("Using existing text file at " + f.getAbsolutePath());
        }

        if (!f.exists()) throw new IOException("File does not exist: " + fileLocation);    //Download problem?//下载有问题报异常

        String allData = getDataAsString(fileLocation);//又嵌套了个函数，跳过去看下

        List<String> list = new ArrayList<>();//搞一个list
        int length = allData.length();//计算大字符串长度
        int currIdx = 0;
        while (currIdx + sequenceLength < length) {//如果当前索引加字符长度小于总长度
            int end = currIdx + sequenceLength;//循环计算字符序列尾索引
            String substr = allData.substring(currIdx, end);//截取串
            currIdx = end;//把结尾索引赋值给新的当前索引
            list.add(substr);//往list添加长度为1000的字符串
        }
        return list;//返回list
    }

    /**
     * Load data from a file, and remove any invalid characters.//加载数据，过滤无效字符，返回大字符串
     * Data is returned as a single large String
     */
    private static String getDataAsString(String filePath) throws IOException {
        List<String> lines = Files.readAllLines(new File(filePath).toPath(), Charset.defaultCharset());//readAllLines这个方法读取文件的所有行,文件字节使用具体的字符集解码成字符，该方法不适合读大文件，第一个参数是文件路径，第二个是用于解码的字符集，返回文件行的列表
        StringBuilder sb = new StringBuilder();//弄一个字符串缓冲
        for (String line : lines) {//把每行弄成一个字符数组，遍历字符数组，如果字符和索引map包含遍历字符，塞进缓冲字符串，这样就起到了过滤的作用，最后返回一个带换行的大字符串
            char[] chars = line.toCharArray();
            for (int i = 0; i < chars.length; i++) {
                if (CHAR_TO_INT.containsKey(chars[i])) sb.append(chars[i]);
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    /**
     * Generate a sample from the network, given an (optional, possibly null) initialization. Initialization
     * can be used to 'prime' the RNN with a sequence you want to extend/continue.<br>
     * Note that the initalization is used for all samples
     *
     * @param initialization     String, may be null. If null, select a random character as initialization for all samples
     * @param charactersToSample Number of characters to sample from network (excluding initialization)
     * @param net                MultiLayerNetwork with one or more GravesLSTM/RNN layers and a softmax output layer
     *///根据给定的参数生成一个范例，初始化可以用于引导rnn按你提供的句子接着往下写
    private static String[] sampleCharactersFromNetwork(String initialization, MultiLayerNetwork net, Random rng,
                                                        Map<Integer, Character> intToChar, int charactersToSample, int numSamples) {//initialization初始化的字符串，可以为空，net是spark网络，rng随机数，intToChar是每个索引对应的字符，
        //  charactersToSample是范例的字符数，就是除了初始化的字符，继续往下写多少个字，numSamples每个训练步完成后写几个例子，这里是4，也就是每个训练步完成写4个例子，每个例子300个字符
        //Set up initialization. If no initialization: use a random character
        if (initialization == null) {//生成第一个字符
            int randomCharIdx = rng.nextInt(intToChar.size());
            initialization = String.valueOf(intToChar.get(randomCharIdx));
        }

        //Create input for initialization
        INDArray initializationInput = Nd4j.zeros(numSamples, intToChar.size(), initialization.length());//生成一个三维数组，第一个参数是写段子的数量，第二个参数是段子词汇索引长度，第三个参数是初始化字符串长度，其实就是1个字符，这和训练样本的shape有所不同
        char[] init = initialization.toCharArray();//把初始化的字符串转成字符数组
        for (int i = 0; i < init.length; i++) {//遍历这个字符数组，如果不给初始字符串，其实只有一个字符，遍历一次
            int idx = CHAR_TO_INT.get(init[i]);//找出初始化字符对应的索引
            for (int j = 0; j < numSamples; j++) {//依次写4个例子
                initializationInput.putScalar(new int[]{j, idx, i}, 1.0f);//通过把不同位置的索引置为1来写
            }
        }

        StringBuilder[] sb = new StringBuilder[numSamples];//可变字符数组
        for (int i = 0; i < numSamples; i++)
            sb[i] = new StringBuilder(initialization);//把初始化字符串放入每个可变字符数组，也就是开头都一样，后面写的不一样

        //Sample from network (and feed samples back into input) one character at a time (for all samples)
        //Sampling is done in parallel here//并行写范文，一边写一遍反馈到输入，一次写一个字符
        net.rnnClearPreviousState();//清理rnn之前的状态参数
        INDArray output = net.rnnTimeStep(initializationInput);//initializationInput是网络的输入，本例中是单时间步，initializationInput的第一个参数代表批大小，第二个参数是输入大小也就是索引大小，第三个参数是1也就是单时间步，output是输出激活函数，和输入的维度一致，这其实相当于设定的预测模式，输入什么样的格式，输出一个什么样的格式


        output = output.tensorAlongDimension(output.size(2) - 1, 1, 0);    //Gets the last time step output//获取最后一个时间步的输出，tensorAlongDimension这个方法改变向量的维度，第一个参数是要改变向量的索引，后两个参数是要改成的维度，这里output.size(2)的意思是取output第二个维度的大小是1，改成1行0列的形式，也就是把每个范例弄成一行，相当于转置


        for (int i = 0; i < charactersToSample; i++) {//开始写300个字符
            //Set up next input (single time step) by sampling from previous output//根据之前的输出设置下一个输入
            INDArray nextInput = Nd4j.zeros(numSamples, intToChar.size());//搞一个多为数组，行数4，列数是字符索引数
            //Output is a probability distribution. Sample from this for each example we want to generate, and add it to the new input//输出是概率分布，根据这个产生样例，并添加到新的输入，具体看看下面代码
            for (int s = 0; s < numSamples; s++) {//每个样例
                double[] outputProbDistribution = new double[intToChar.size()];//搞一个字符索引长度的概率分布数组
                for (int j = 0; j < outputProbDistribution.length; j++)//对每个概率
                    outputProbDistribution[j] = output.getDouble(s, j);//获取该样例该位置的概率
                int sampledCharacterIdx = sampleFromDistribution(outputProbDistribution, rng);//函数sampleFromDistribution的作用是从分布中选出索引


                nextInput.putScalar(new int[]{s, sampledCharacterIdx}, 1.0f);        //Prepare next time step input//写入下一个输入
                sb[s].append(intToChar.get(sampledCharacterIdx));    //Add sampled character to StringBuilder (human readable output)//根据索引获得字符添加到对应缓冲数组
            }

            output = net.rnnTimeStep(nextInput);    //Do one time step of forward pass//向前做一个时间步
        }

        String[] out = new String[numSamples];//搞4个样例字符串数组
        for (int i = 0; i < numSamples; i++) out[i] = sb[i].toString();//把每个样例转成字符串写进去并返回
        return out;
    }

    /**
     * Given a probability distribution over discrete classes, sample from the distribution
     * and return the generated class index.//获取一个概率分布，从中抽样并返回产生类的索引
     *
     * @param distribution Probability distribution over classes. Must sum to 1.0
     */
    private static int sampleFromDistribution(double[] distribution, Random rng) {//传入分布数组和随机生成器
        double d = rng.nextDouble();
        double sum = 0.0;
        for (int i = 0; i < distribution.length; i++) {//遍历分布数组，累加分布值知道大于等于随机数，这时返回索引，这说明先遇到较大概率的索引容易被选中
            sum += distribution[i];
            if (d <= sum) return i;
        }
        //Should never happen if distribution is a valid probability distribution
        throw new IllegalArgumentException("Distribution is invalid? d=" + d + ", sum=" + sum);
    }

    /**
     * A minimal character set, with a-z, A-Z, 0-9 and common punctuation etc
     */
    private static char[] getValidCharacters() {
        List<Character> validChars = new LinkedList<>();//搞一个字符list
        for (char c = 'a'; c <= 'z'; c++) validChars.add(c);//用a-z，A-Z，0-9和特殊字符填充list
        for (char c = 'A'; c <= 'Z'; c++) validChars.add(c);
        for (char c = '0'; c <= '9'; c++) validChars.add(c);
        char[] temp = {'!', '&', '(', ')', '?', '-', '\'', '"', ',', '.', ':', ';', ' ', '\n', '\t'};
        for (char c : temp) validChars.add(c);
        char[] out = new char[validChars.size()];//搞一个新的字符数组
        int i = 0;
        for (Character c : validChars) out[i++] = c;//把list的内容放到数组里
        return out;
    }

    public static Map<Integer, Character> getIntToChar() {
        Map<Integer, Character> map = new HashMap<>();//搞一个map
        char[] chars = getValidCharacters();//获取有效字符，调用函数，可以先看函数
        for (int i = 0; i < chars.length; i++) {
            map.put(i, chars[i]);//以索引为主键，字符为value填充map
        }
        return map;
    }

    public static Map<Character, Integer> getCharToInt() {
        Map<Character, Integer> map = new HashMap<>();//搞一个map
        char[] chars = getValidCharacters();//获取有效字符，调用函数，可以先看函数
        for (int i = 0; i < chars.length; i++) {//遍历字符数组
            map.put(chars[i], i);//以字符为主键，索引为value填充map
        }
        return map;
    }
}