package com.evayInfo.Inglory.NLP.HanLP

import com.hankcs.hanlp.HanLP

object NameRecognizeDemo2 {

  def main(args: Array[String]): Unit = {

    val txt = "收藏 查看我的收藏 0 有用+1 已投票 0 徐斌 （同济大学教授） 编辑 锁定 本词条缺少名片图，补充相关内容使词条更完整，还能快速升级，赶紧来编辑吧！ 徐斌是同济大学环境科学与工程学院，市政工程系博士生导师，教授。主要研究方向:饮用水处理技术、消毒副产物生成与控制、微量有机物的高级氧化去除。主讲课程：给水排水仪表与自动控制、给水工程、专业外语。 中文名 徐斌 主要研究方向 饮用水处理技术 职    务 市政工程系博士生导师 毕    业 同济大学 目录 1 个人简介 2 学术贡献 徐斌个人简介 编辑 同济大学环境科学与工程学院，市政工程系博士生导师，教授 1994.9～1998.7 南昌大学 环境工程系，环境工程专业，获得工学学士学位； 1998.9～2001.3 东华大学（原中国纺织大学）环境科学与工程学院，环境工程专业，获得工学硕士学位； 2001.3～2003.6 同济大学 环境科学与工程学院，环境工程专业，获工学博士学位。 2003.7～2005.7 同济大学环境科学与工程学院 市政工程，博士后 2005.3～2005.6法国国家科研中心（CNRS）化学工程科学实验室（LSGC）， 博士后 2005.7～2006.6 同济大学环境科学与工程学院市政工程系任教，讲师 2006.6～2011.6 同济大学环境科学与工程学院任教，副教授、硕士生导师 2007.8～2008.9 美国斯坦福大学市政与环境工程系，访问学者 2011.6～2011.12 同济大学环境科学与工程学院任教，副教授、博士生导师 2011.9～2012.8 住房和城乡建设部建筑节能与科技司，借调工作 2011.12至今 同济大学环境科学与工程学院任教，教授、博士生导师 [1]  徐斌学术贡献 编辑 1.Bin Xu, Cao Qin, et al, Degradation kinetics and N-Nitrosodimethylamine formation duringmonochloramination of chlortoluron, Sci Total Environ, 2012, 417-418: 241-247 2. Bin Xu, Da-peng Li, Wei Li, Sheng-Ji Xia, Yi-Li Lin etc; Measurements of dissolved organic nitrogen (DON) in water samples with nanofiltration pretreatment; Water research; 2010, 44(18): 5376-5384 3. Bin Xu, Tao Ye, Da-peng Li, Chen-yan Hu, Yi-Li Lin, Sheng-ji Xia, Nai-yun Gao；Measurement of dissolved organic nitrogen in a drinking water treatment plant: Size fraction，fate, and relation to water quality parameters; Science of the Total Environment, 2011, 409: 1116–1122 4. Bin Xu, Fu-Xiang Tian, Chen-Yan Hu, Yi-Li Lin, Sheng-Ji Xia, Rong Rong, Da-Peng Li, Chlorination of chlortoluron: Kinetics, pathways and chloroform formation, Chemosphere, 2011,83: 909–916 5. Bin Xu, Nai-yun Gao, Hefa Cheng, Sheng-ji Xia etc; Oxidative degradation of dimethyl phthalate (DMP) by UV/H2O2 process; Journal of Hazardous Materials; 2009, 162, 954–959 6. Bin Xu, He-Zhen Zhu, Yi-Li Lin etc; Formation of Volatile Halogenated By-Products During the Chlorination of Oxytetracycline, Water Air Soil Pollut; 2012; 223:4429-4436 7. Bin Xu, Nai-yun Gao, Hefa Cheng, Sheng-ji Xia etc; Ametryn degradation by aqueous chlorine: Kinetics and reaction influences; Journal of Hazardous Materials ; 2009, 169, 586-592 8. Xu Bin, Gao Nai-yun, Sun Xiao-feng etc; Photochemical Degradation of Diethyl Phthalate with UV/H2O2, Journal of Hazardous Materials, 2007, 139(1-2) 132-139 9. XU Bin, GAO Nai-yun, Sun Xiao-feng, Xia Sheng-ji etc; Characteristics of Organic Materials in Huangpu River and Treatabilities of O3-BAC Process, Separation and Purification Technology, 2007, 57(2):348-355 10. Bin Xu, Tingyao Gao, Chenyan Hu, etc. The Wind-frequency Allocation Method on Discharge loading in Function Zone. Journal of The Air & Waste Management Association; 2002, Vol 52(6), p714-718. 11. Xu Bin, Gao Naiyun , Rui Min etc；Degradation of endocrine disruptor bisphenol A in drinking water by ozone oxidation，Frontiers of Environmental Science & Engineering in China, 2007, Vol 1（3）:1673-7415 12. 高乃云、严敏、赵建夫、徐斌著；《水中内分泌干扰物处理技术与原理》，中国建筑工业出版社，2010年 13. 高乃云、楚文海、严敏、徐斌著；《饮用水消毒副产物形成与控制研究》，中国建筑工业出版社，2012年 [1]  参考资料 1.    徐斌教授个人简介  ．同济大学环境科学与工程学院官方网站——师资介绍[引用日期2013-03-11]"

    val segment = HanLP.newSegment().
      enableNameRecognize(true). // 识别人名
      enablePlaceRecognize(true). // 识别地名
      enableOrganizationRecognize(true) // 识别机构名

    val termList = segment.seg(txt)
    for(i <- 0 to termList.size()-1) {
      if (termList.get(i).nature.startsWith("nr")) {
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

    // 短语提取
    val phraseList = HanLP.extractPhrase(txt, 10)
    println(s"短语提取结果为：$phraseList")

  }
}
