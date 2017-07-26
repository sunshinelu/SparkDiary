
# 舆情分析分析流程

## 一、数据抓取分工：

杨春：论坛贴吧、搜索引擎、网站门户

孙伟：微博、博客

平高：微信




## 二、数据清洗方案：

各表所在数据库：

ycIngloryBDP:`DA_BBSARTICLE`、`DA_BBSCOMMENT`、`DA_BAIDUARTICLE`


IngloryBDP:`DA_WEIBO`、`DA_WEIBO_COMMENTS`、`DA_WEIXIN`、`DA_SEED`、`DA_BLOG`


### (一)、微博：

1. 微博数据表结构：

`DA_WEIBO`：
  `ID`：微博ID
  `TEXT`：微博内容
  `REPOSTSCOUNT`：转发数
  `COMMENTSCOUNT`：评论数
  `CREATEDAT`：发表时间
  `UID`：微博作者ID
  `TITLE`：标题
  `WEIBO_KEY`：关键字
   `SYSTIME`：系统时间



`DA_WEIBO_COMMENTS`：
  `ID`：评论ID
  `TEXT`：评论内容
  `WEIBO_ID`：微博ID
  `CREATED_AT`： 发表时间
  `UID`： 评论人ID
  `SCREEN_NAME`：评论人昵称
  `SOURCE`：来源设备


2. 新浪微博中常见的符号有三种：@  #  //

1) `@微博人名`：@这个符号会把紧跟它之后的文字当成一个人名，自动生成一个指向这个人的微博。当然如果这个微博名字不存在，系统会告诉你。

2) `#搜索#`：#是成双出现的，夹在两个#之间的文字会成为一个搜索关键字。

3) `//转帖`：//和前二者不同，它不具有链接功能，只是标注这个微博经过了哪几个人转发而来的。一般这个符号是自动生成的，倒不用学怎么使用，但会看还是有必要的。

参考链接：https://wenku.baidu.com/view/0230a8fd941ea76e58fa04cb.html

3. 微博数据`DA_WEIBO`清洗流程

0) 使用Jsoup提取数据`Jsoup.parse(content).body().text()`

1) 删除数据中的表情字符串：表情字符串正则为`"\\[[0-9a-zA-Z\\u4e00-\\u9fa5]+\\]"`。

2) 提取数据中的转发字符串`//@用户名`：转发字符串正则为`//@[\u4e00-\u9fa5a-zA-Z0-9_-]+[\u4e00-\u9fa5a-zA-Z0-9_：【】,.?:;'"!，。！“”；？]+`

3) 提取数据中`@的用户`：先将转发的数据替换成空，然后使用正则提取@用户，提取@用户的正则为`"@[^,，：:\\s@]+"` 或者`"@[\\u4e00-\\u9fa5a-zA-Z0-9_-]{4,30}"`，在提取@用户名时使用`"@[\\u4e00-\\u9fa5a-zA-Z0-9_-]{4,30}"`相对较好。

4) 提取数据中的`#话题#`：提取#话题#的正则为：`"#[^#]+#"`。

5) 提取微博正文：删除数据中的表情字符串之后，使用正则将转发、@用户名、#话题#等替换成空，该正则为`"//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+[\\u4e00-\\u9fa5a-zA-Z0-9_：【】,.?:;'\"!，。！“”；？]+|@[^,，：:\\s@]+|#[^#]+#"`。


4. 微博数据`DA_WEIBO_COMMENTS`清洗流程

0) 使用Jsoup提取数据`Jsoup.parse(content).body().text()`

1) 删除数据中的表情字符串：表情字符串正则为`"\\[[0-9a-zA-Z\\u4e00-\\u9fa5]+\\]"`。

2) 删除`回复@用户名:`：“回复@用户名”正则为`"回复@[^,，：:\\s@]+[:：]"`。

3) 提取微博正文：删除数据中的表情字符串之后，使用正则将转发、@用户名、#话题#等替换成空，该正则为`"//@[\\u4e00-\\u9fa5a-zA-Z0-9_-]+[\\u4e00-\\u9fa5a-zA-Z0-9_：【】,.?:;'\"!，。！“”；？]+|@[^,，：:\\s@]+|#[^#]+#"`。


### (二)、微信

1. 微信数据表结构：

`DA_WEIXIN`：
  `WX_ID`：文章唯一标识
  `WX_KEY`：加密后的ID
  `WX_URL`：微信文章地址
  `WX_TITLE`：微信文章标题
  `WX_DATE`：微信文章时间
  `WX_CONTENT`：微信文章内容
  `WX_APPC`：带样式的内容
  `WX_USER`
  `WX_TASK`：微信采集id
  `WX_IMG`
  `WX_ZT`：主题
  `CREATE_TIME`：系统时间
  `DEL_FLAG`





### (三)、论坛

1. 论坛数据表结构：

`DA_BBSARTICLE`文章表：
  `ID`：文章ID
  `TITLE`：标题
  `CONTENT`：内容
  `AUTHOR`：作者
  `TIME`：发布时间
  `CLICKNUM`：点击数
  `REPLY`：回复数
  `KEYWORD`：主题
  `BZ`：备注
  `TASKID`
  `APPC`：带样式的正文
   `URL`：源网页地址
   `CREATTIME`：系统时间


`DA_BBSCOMMENT`评论表：
  `ID`：评论ID
  `ARTICLEID`：对应文章表中的文章id
  `JSUSERNAME`：评论作者
  `JSRESTIME`：评论时间
  `FLOORID`：楼
  `BBSCONTENT`：评论的内容
  `CREATETIME`：系统时间

### (四)、搜索引擎

`DA_BAIDUARTICLE`
  `ID`：文章ID
  `CONTENT`：正文
  `TITLE`：标题
  `TIME`：时间
  `KEYWORD`：关键词
  `SOURCE`：源
  `TASKID` 
  `SOURCEURL`：源url
  `CHARSET`：编码
  `CREATETIME`：系统时间



### (五)、网站门户


`DA_SEED`
  `SEED_ID`：序号
  `SEED_URL`：采集地址
  `SEED_TITLE`：标题
  `SEED_APPC`：带样式的内容
  `SEED_CONTENT`：内容
  `SEED_DATE`：时间
  `TASK_ID`：任务id
  `CREATE_BY`：创建人
  `CREATE_TIME`：创建时间（系统时间）
  `UPDATE_BY`：修改人
  `UPDATE_TIME`：修改时间
  `DEL_FLAG`：删除标记 1:正常  2:删除
  `MANUALLABEL`：标签
  `TYPE`：区分网站、微信、微博
  `FJFLAG`：标注是否为附件
  `SOURCEURL`：源网页地址


### (六)、博客

`DA_BLOG`
  `ID`：文章ID
  `TITLE`：标题
  `CONTENT`：正文
  `CONTENT_HTML`:带格式的正文
  `URL`：链接地址
  `CREATEDAT`：发表时间
  `AUTHOR`：博主
  `BLOG_KEY`：标签
  `SYSTIME`：系统时间
  `TASK_ID`：任务ID


有内容格式的表：
`DA_SEED`、`DA_BBSARTICLE`、`DA_WEIXIN`、`DA_WEIBO`、`DA_WEIBO_COMMENTS`

没有内容格式的表：
`DA_BLOG`、`DA_BAIDUARTICLE`、`DA_BBSCOMMENT`


有原网链接的表：
`DA_BLOG`、`DA_SEED`、`DA_BAIDUARTICLE`、`DA_BBSARTICLE`、`DA_WEIXIN`

没有原网链接的表：
`DA_BBSCOMMENT`、`DA_WEIBO`、`DA_WEIBO_COMMENTS`



## 三、情感分析流程：

# 1. 数据获取

情感分析所需数据为：ID、关联I、标题、内容、来源、来源UR、关键词、打分、时间、是否是评论

1) `DA_WEIBO`中获取的数据为：
  `ID`（微博ID）
   `TITLE`（标题）
   `TEXT`（微博内容）
   `CREATEDAT`（发表时间）
   `WEIBO_KEY`（关键字）
   新增一列`SOURCE`（来源）列：来源为`WEIBO`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
   

2) `DA_WEIBO_COMMENTS`中获取的数据为：
  `ID`（评论ID）
  `WEIBO_ID`：微博ID
  `TITLE`（标题）：通过`WEIBO_ID`从`DA_WEIBO`表中`TITLE`列获取。
  `TEXT`（评论内容）
  `CREATED_AT`： 发表时间
  `WEIBO_KEY`（关键字）：通过`WEIBO_ID`从`DA_WEIBO`表中`WEIBO_KEY`列获取。
   新增一列`SOURCE`（来源）列：来源为`WEIBO`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是


3) `DA_WEIXIN`中获取数据为：
  `WX_ID`（文章唯一标识）
   `WX_TITLE`（微信文章标题）
  `WX_DATE`（微信文章时间）
  `WX_CONTENT`（微信文章内容）
  `WX_APPC`：带样式的内容
  `WX_ZT`（主题）
  `WX_URL`：微信文章地址
   新增一列`SOURCE`（来源）列：来源为`WEIXIN`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
 
 
4) `DA_BBSARTICLE`文章表中获取的数据为：
  `ID`（文章ID）
  `TITLE`（标题）
  `CONTENT`（内容）
  `APPC`：带样式的正文
  `URL`：源网页地址
  `TIME`（发布时间）
  `KEYWORD`（主题）
   新增一列`SOURCE`（来源）列：来源为`LUNTAN`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是



5) `DA_BBSCOMMENT`评论表中获取的数据为：
  `ID`（评论ID）
  `ARTICLEID`（对应文章表中的文章id）
  `TITLE`（标题）：通过`ARTICLEID`从`DA_BBSARTICLE`表中`TITLE`列获取
  `JSRESTIME`（评论时间）
  `BBSCONTENT`（评论的内容）进行数据清洗后结果
   新增一列`KEYWORD`（主题）：通过`ARTICLEID`从`DA_BBSARTICLE`表中`KEYWORD`列获取。
   新增一列`SOURCE`（来源）列：来源为`LUNTAN`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是
   
   
   
6) `DA_BAIDUARTICLE`
  `ID`：文章ID
  `CONTENT`：正文
  `TITLE`：标题
  `TIME`：时间
  `KEYWORD`：关键词
  `SOURCEURL`：源url
   新增一列`SOURCE`（来源）列：来源为`SEARCH`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是

  
7) `DA_SEED`
     `SEED_ID`：序号
     `SEED_TITLE`：标题
     `SEED_CONTENT`：内容
     `SEED_APPC`：带样式的内容
     `SEED_DATE`：时间
     `MANUALLABEL`：标签
     `SOURCEURL`：源网页地址
     新增一列`SOURCE`（来源）列：来源为`MENHU`
     新增一列`IS_COMMENT`：是否是评论, 0：否 1：是



8) `DA_BLOG`
  `ID`：文章ID
  `TITLE`：标题
  `CONTENT`：正文
  `CONTENT_HTML`:带格式的正文
  `CREATEDAT`：发表时间
  `BLOG_KEY`：标签
  `URL`：链接地址
   新增一列`SOURCE`（来源）列：来源为`BLOG`
   新增一列`IS_COMMENT`：是否是评论, 0：否 1：是


## 四、情感分析结果：

### 表中信息：
文章的ID：
文章时间：年－月－日 时－分－秒
分析时间：年－月－日 时－分－秒
情感打分：
文章主题：

`sentiment_trend`（情感分析表）：9项
  `ARTICLEID`：文章id
  `TIME`：文章时间
  `SYSTIME`：分析时间
  `SCORE`：文章得分
  `KEYWORD`：标签（主题）
  `SOURCE`:来源
  `TITLE`：文章标题
  `TEXT`:文章内容（不清洗）
  `LABEL`:label列


### 数据清洗结果表：（暂时不要）

 `ARTICLEID`：文章id
  文章的标题
  文章内容：不清洗
  文章内容：已清洗的
  `TIME`：文章时间
  `KEYWORD`：标签（主题）
  `SOURCE`:来源


## 五、最新舆情




## 六、严重舆情


## 七、最终结果表

`SUMMARYARTICLE`
  `ARTICLEID`：文章id
  `TITLE`：文章标题
  `CONTENT`：文章内容
  `SOURCE`：文章来源
  `KEYWORD`：标签:台湾  扶贫
  `SCORE` ：文章得分
  `LABEL`：标签：正类、负类、中性、严重
  `TIME`：文章发表时间
  `SYSTIME`：分析时间
 `IS_COMMENT`：是否是评论 0：否 1：是 

弃用`SUMMARYARTICLE`表。


`yq_article`
  `id`：主键
  `articleId`：文章(评论)id
  `glArticleId`：评论关联的文章id
  `title`：标题
  `source`：来源
  `sourceurl`：原网页地址
  `keyword`：标签:台湾  扶贫
  `score`：分值
  `label`：标签：正类、负类、中性、严重
  `time`：发表时间
  `systime`：分析时间
  `is_comment`：是否是评论 0：否 1：是



`yq_content` 
  `articleId`：文章id
  `content`：正文


微博和论坛的评论中的内容没有格式，那么微博和论坛的评论的APPC列为NUL，在yq_content中则会出现content列为null的情况。

`yq_article`表中的`articleId`列只有微博和论坛的评论有数据，其它均为null。
