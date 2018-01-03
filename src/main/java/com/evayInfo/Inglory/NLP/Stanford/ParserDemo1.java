package com.evayInfo.Inglory.NLP.Stanford;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.trees.Tree;

/**
 * Created by sunlu on 18/1/3.
 */
public class ParserDemo1 {
    public static void main(String[] args) throws IOException {
//      String grammar = "edu/stanford/nlp/models/lexparser/chineseFactored.ser.gz";
        String grammar = "edu/stanford/nlp/models/lexparser/chinesePCFG.ser.gz";
        String[] options = {};
        LexicalizedParser lp = LexicalizedParser.loadModel(grammar, options);
        String line = "我 的 名字 叫 小明 ？";
        Tree parse = lp.parse(line);
        parse.pennPrint();
        /*
        String[] arg2 = {"-encoding", "utf-8",
                "-outputFormat", "penn,typedDependenciesCollapsed",
                "edu/stanford/nlp/models/lexparser/chineseFactored.ser.gz",
                "/home/shifeng/shifengworld/study/tool/stanford_parser/stanford-parser-full-2015-04-20/data/chinese-onesent-utf8.txt"};
        LexicalizedParser.main(arg2);
        */
    }
}
