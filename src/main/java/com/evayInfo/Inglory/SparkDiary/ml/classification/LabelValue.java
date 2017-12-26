package com.evayInfo.Inglory.SparkDiary.ml.classification;

import java.io.Serializable;

/**
 * Created by sunlu on 17/12/26.
 * 参考链接：
 Spark2.0 特征提取、转换、选择之二：特征选择、文本处理，以中文自然语言处理(情感分类为例)
 http://blog.csdn.net/qq_34531825/article/details/52431264
 *
 */

public class LabelValue implements Serializable {
    private String value;
    private double label;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public double getLabel() {
        return label;
    }

    public void setLabel(double label) {
        this.label = label;
    }

}