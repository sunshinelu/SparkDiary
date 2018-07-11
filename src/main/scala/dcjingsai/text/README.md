“达观杯”文本智能处理挑战赛


数据集：“达观杯”文本智能处理挑战赛

链接：http://www.dcjingsai.com/common/cmpt/%E2%80%9C%E8%BE%BE%E8%A7%82%E6%9D%AF%E2%80%9D%E6%96%87%E6%9C%AC%E6%99%BA%E8%83%BD%E5%A4%84%E7%90%86%E6%8C%91%E6%88%98%E8%B5%9B_%E7%AB%9E%E8%B5%9B%E4%BF%A1%E6%81%AF.html?slxydc=a8be54

需求：
构建文本分类模型


数据下载地址：
地址：https://pan.baidu.com/s/13IMDPMz0rf8kM1JAea53uQ
密码：y6m4


数据描述：
数据包含2个csv文件：

train_set.csv：此数据集用于训练模型，每一行对应一篇文章。文章分别在“字”和“词”的级别上做了脱敏处理。共有四列：
第一列是文章的索引(id)，第二列是文章正文在“字”级别上的表示，即字符相隔正文(article)；第三列是在“词”级别上的表示，即词语相隔正文(word_seg)；第四列是这篇文章的标注(class)。
注：每一个数字对应一个“字”，或“词”，或“标点符号”。“字”的编号与“词”的编号是独立的！

test_set.csv：此数据用于测试。数据格式同train_set.csv，但不包含class。
注：test_set与train_test中文章id的编号是独立的。

