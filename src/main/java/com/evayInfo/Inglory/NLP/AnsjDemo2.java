package com.evayInfo.Inglory.NLP;

import org.ansj.domain.Term;
import org.ansj.recognition.NatureRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sunlu on 17/10/12.
 */
public class AnsjDemo2 {
    public static void main(String[] args) {

        List<Term> terms = ToAnalysis.parse("Ansj中文分词是一个真正的ict的实现.并且加入了自己的一些数据结构和算法的分词.实现了高效率和高准确率的完美结合!");
        System.out.println("terms is: " + terms);
        new NatureRecognition(terms).recognition(); //词性标注
        System.out.println(terms);

        String[] strs = {"对", "非", "ansj", "的", "分词", "结果", "进行", "词性", "标注"};
        List<String> lists = Arrays.asList(strs);
        List<Term> recognition = NatureRecognition.recognition(lists, 0);
        System.out.println(recognition);
    }
}
