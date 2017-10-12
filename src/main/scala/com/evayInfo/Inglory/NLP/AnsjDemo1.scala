package com.evayInfo.Inglory.NLP

import java.util

import org.ansj.app.keyword.{KeyWordComputer, Keyword}
import org.ansj.recognition.NatureRecognition
import org.ansj.splitWord.analysis.ToAnalysis

/**
 * Created by sunlu on 17/10/12.
 */
object AnsjDemo1 {

  def main(args: Array[String]) {

    val kwc = new KeyWordComputer(5)
    val title = "维基解密否认斯诺登接受委内瑞拉庇护"
    val content = "有俄罗斯国会议员，9号在社交网站推特表示，美国中情局前雇员斯诺登，已经接受委内瑞拉的庇护，不过推文在发布几分钟后随即删除。俄罗斯当局拒绝发表评论，而一直协助斯诺登的维基解密否认他将投靠委内瑞拉。　　俄罗斯国会国际事务委员会主席普什科夫，在个人推特率先披露斯诺登已接受委内瑞拉的庇护建议，令外界以为斯诺登的动向终于有新进展。　　不过推文在几分钟内旋即被删除，普什科夫澄清他是看到俄罗斯国营电视台的新闻才这样说，而电视台已经作出否认，称普什科夫是误解了新闻内容。　　委内瑞拉驻莫斯科大使馆、俄罗斯总统府发言人、以及外交部都拒绝发表评论。而维基解密就否认斯诺登已正式接受委内瑞拉的庇护，说会在适当时间公布有关决定。　　斯诺登相信目前还在莫斯科谢列梅捷沃机场，已滞留两个多星期。他早前向约20个国家提交庇护申请，委内瑞拉、尼加拉瓜和玻利维亚，先后表示答应，不过斯诺登还没作出决定。　　而另一场外交风波，玻利维亚总统莫拉莱斯的专机上星期被欧洲多国以怀疑斯诺登在机上为由拒绝过境事件，涉事国家之一的西班牙突然转口风，外长马加略]号表示愿意就任何误解致歉，但强调当时当局没有关闭领空或不许专机降落。";
    val result: util.Collection[Keyword] = kwc.computeArticleTfidf(title, content)
    println(result)
    //[斯诺登/211.83897497289786, 维基/163.46869316143392, 委内瑞拉/101.31414008144232, 庇护/46.05172894231714, 俄罗斯/45.70875018647603]

    val result2 = kwc.computeArticleTfidf(title, content).toArray.map(_.toString.split("/")).
      filter(_.length >= 2).map(_ (0)).toList.filter(word => word.length >= 2)
    println("result2 is:" + result2)

    val getKeyWords = kwc.computeArticleTfidf(title, content).toArray.map(_.toString.split("/")).
      filter(_.length >= 2) //.map{x => (x(0).mkString(";"),x(1).mkString(";"))}//.toList.filter(word => word.length >= 2)
    val keyWord = getKeyWords.map(_ (0)).toList
    val weight = getKeyWords.map(_ (1)).toList
    println(keyWord)
    println(weight)
    val getKeyWords2 = kwc.computeArticleTfidf(title, content)
    println(getKeyWords2.toString)




    //    val strs: Array[String] = Array("对", "非", "ansj", "的", "分词", "结果", "进行", "词性", "标注")
    //    val lists: List[String] = strs.toList()//Arrays.asList(strs)
    //
    //    val recognition: List[Term] = NatureRecognition.recognition(result2, 0)
    //    System.out.println(recognition)
    //    println(recognition)

    val terms = ToAnalysis.parse("Ansj中文分词是一个真正的ict的实现.并且加入了自己的一些数据结构和算法的分词.实现了高效率和高准确率的完美结合!");
    new NatureRecognition(terms).recognition() //词性标注
    println(terms)


  }

}
