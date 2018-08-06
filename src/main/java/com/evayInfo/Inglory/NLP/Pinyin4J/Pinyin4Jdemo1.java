package com.evayInfo.Inglory.NLP.Pinyin4J;

import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;

/**
 * Created by sunlu on 18/8/2.
 */
public class Pinyin4Jdemo1 {
    public static void main(String[] args) {
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);

        String str = "我爱自然语言处理,Keyven";
        System.out.println(str);
        String[] pinyin = null;
        for (int i = 0; i < str.length(); ++i) {
            try {
                pinyin = PinyinHelper.toHanyuPinyinStringArray(str.charAt(i),
                        format);
            } catch (BadHanyuPinyinOutputFormatCombination e) {
                e.printStackTrace();
            }

            if (pinyin == null) {
                System.out.print(str.charAt(i));
            } else {
                if (i != 0) {
                    System.out.print(" ");
                }
                System.out.print(pinyin[0]);
            }
        }
    }
}
