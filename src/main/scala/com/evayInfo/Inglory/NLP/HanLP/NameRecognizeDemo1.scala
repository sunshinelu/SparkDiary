package com.evayInfo.Inglory.NLP.HanLP

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer


object NameRecognizeDemo1 {
  def main(args: Array[String]): Unit = {

    val testCase = Seq(
//      "签约仪式前，秦光荣、李纪恒、仇和等一同会见了参加签约的企业家。",
//      "王国强、高峰、汪洋、张朝阳光着头、韩寒、小四",
//      "张浩和胡健康复员回家了",
//      "王总和小丽结婚了",
//      "编剧邵钧林和稽道青说",
//      "这里有关天培的有关事迹",
//      "龚学平等领导,邓颖超生前",
//      "俞汝勤（1935－），分析化学家，中国科学院院士。湖南长沙人。毕业于长沙雅礼中学。1953年入苏联列宁格勒矿业学院化学系学习，1959年毕业。回国后在中科院长春应用化学研究所工作。1962年调湖南大学，历任化工系教授、湖南大学校长等职。1984年加入共产党，现任中国化学学会理事、《化学传感器》主编。专长于有机分析试剂和化学计量学研究，所主持的“氟离子选择性电极研究”和“有机试剂用于电化学及催化动力分析研究”，分别获1978年全国科学大会奖和1987年国家自然科学奖。著有《现代分析化学与信息理论基础》、《化学计量学导论》等。1991年当选为中国科学院化学部委员，1994年改称院士。",
//    "卢桂华，讲师，硕士。1992年毕业于重庆大学外语系获学士学位，留校任教。2002年于重庆大学外国语学院外国语言学及应用语言学翻译方向毕业，获硕士学位，2011-2012年英国曼彻斯特大学翻译及跨文化研究中心访问学者。\n\n主要研究方向：翻译和英语教学\n\n主要教授课程：综合英语，翻译理论与实践，时文阅读，英语听力等"),
//    "淄博市人民防空办公室综合训练教育基地内配项目家具采购需求公示 一、项目概况及预算情况： 本项目为淄博市人民防空综合训练教育基地内配项目家具采购，共1个包，项目预算168.23万元。 二、采购标的具体情况： 详见附件 三、论证意见： 无 四、公示时间：本项目采购需求公示期限为3天：自2019年6月4日起，至2019年6月7日止 五、意见反馈方式： 本项目采购需求方案公示期间接受社会公众及潜在供应商的监督。 请遵循客观、公正的原则，对本项目需求方案提出意见或者建议，并请于2019-06-08前将书面意见反馈至采购人或者采购代理机构，采购人或者采购代理机构应当于公示期满5个工作日内予以处理。 采购人或者采购代理机构未在规定时间内处理或者对处理意见不满意的，异议供应商可就有关问题通过采购文件向采购人或者采购代理机构提出质疑；质疑未在规定时间内得到答复或者对答复不满意的，异议供应商可以向采购人同级财政部门提出投诉。 六、项目联系方式 1、采购单位：淄博市人民防空办公室 地址：张店区联通路106号 联系人：朱科长 联系方式：0533-2722536 2.采购代理机构：山东普惠招标有限公司 地址：山东省淄博市张店区西二路128 联系人：孙德萌 联系方式：0533-6217863 附件：『淄博市人民防空综合训练教育基地内配项目家具采购需求方案定.doc』 发布人：山东普惠招标有限公司管理员 发布时间：2019年06月04日 16时02分36秒 相关信息 1、淄博市人民防空办公室淄博市人民防空综合训练教育基地内配项目家具采购需求公开",
    "标包货物服务名称供应商名称地址成交结果A威海市重大项目管理服务平台宁波中经科技发展有限公司宁波市高新区光华路299弄9幢19号10楼99.2万")

  val segment = HanLP.newSegment().
    enableNameRecognize(true). // 识别人名
    enablePlaceRecognize(true). // 识别地名
    enableOrganizationRecognize(true) // 识别机构名



//    testCase.foreach(x => segment.seg(x))

  for(sentence <- testCase){
    val termList = segment.seg(sentence.toString)
    println(termList)
    for(i <- 0 to termList.size()-1){
      if(termList.get(i).nature.startsWith("nr")){
        val name = termList.get(i).word
        println(s"人名是：$name")
      }

      if(termList.get(i).nature.startsWith("ns")){
        val local = termList.get(i).word
        println(s"地点名是：$local")
      }

      if(termList.get(i).nature.startsWith("ni")){
        val jigou = termList.get(i).word
        println(s"机构名是：$jigou")
      }

      if(termList.get(i).nature.startsWith("nt")){
        val jigoutt = termList.get(i).word
        println(s"机构团体名是：$jigoutt")
      }

      if(termList.get(i).nature.startsWith("g")){
        val xueshu = termList.get(i).word
        println(s"学术词汇是：$xueshu")
      }
    }
  }


    // 短语提取
    val phraseList = HanLP.extractPhrase(testCase(0), 10)
    println(s"短语提取结果为：$phraseList")

    // 自动摘要

    val sentenceList = HanLP.extractSummary(testCase(0), 3)
    println(s"自动摘要结果为：$sentenceList")

    // CRF分词
//    val analyzer = new CRFLexicalAnalyzer()
//    println(analyzer.analyze(testCase(0)))




  }

}
