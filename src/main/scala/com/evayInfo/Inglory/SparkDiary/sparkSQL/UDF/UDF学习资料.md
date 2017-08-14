# UDF学习资料

> 参考链接：
>
> spark dataframe 新增列
> <https://my.oschina.net/whx403/blog/826651>


以UDF方式新增列

    val cmsUrlPatten = """(^http[s]?://.+_)[0-9]+\.(shtml|html){1}.*""".r


    def url2cms(url: String): String = {
        val tagUrl = url.split("\\?")(0)
        if (cmsUrlPatten.findAllIn(tagUrl).isEmpty) url
        else {
          val cmsUrlPatten(urlPre, urlEnd) = tagUrl
          s"${urlPre}1.$urlEnd"
        }
      }

    val sqlfunc = udf((arg: String) => url2cms(arg))

    trafficDF.withColumn("cmsUrl", sqlfunc($"url"))