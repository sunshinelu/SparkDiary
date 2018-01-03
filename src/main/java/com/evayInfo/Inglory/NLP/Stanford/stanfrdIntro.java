package com.evayInfo.Inglory.NLP.Stanford;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import jdk.internal.org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sunlu on 18/1/3.
 */
public class stanfrdIntro {
    public static void main(String[] args) throws IOException, SAXException {

        String serializedClassifier = "classifiers/english.all.3class.distsim.crf.ser.gz";


        AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier
                .getClassifierNoExceptions(serializedClassifier);

        String s1 = "Good afternoon Rahul Kulhari, how are you today?";

        s1 = s1.replaceAll("\\s+", " ");
        String t = classifier.classifyWithInlineXML(s1);
        System.out.println(Arrays.toString(getTagValues(t).toArray()));

    }

    private static final Pattern TAG_REGEX = Pattern.compile("<PERSON>(.+?)</PERSON>");

    private static Set<String> getTagValues(final String str) {
        final Set<String> tagValues = new HashSet<String>();
        //final Set<String> tagValues = new TreeSet();
        final Matcher matcher = TAG_REGEX.matcher(str);
        while (matcher.find()) {
            tagValues.add(matcher.group(1));
        }

        return tagValues;
    }
}
