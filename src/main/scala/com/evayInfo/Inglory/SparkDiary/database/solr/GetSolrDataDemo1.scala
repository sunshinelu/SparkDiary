package com.evayInfo.Inglory.SparkDiary.database.solr

import java.io.{FileOutputStream, IOException, OutputStreamWriter}
import java.util.{ArrayList, List}

import org.apache.solr.client.solrj.{SolrClient, SolrQuery, SolrRequest}
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.params.ModifiableSolrParams

/**
 * Created by sunlu on 17/9/7.
 */
object GetSolrDataDemo1 {

  //  case class SlorSchema(var id: String, var lastModified:Date)

  def getData: List[SlorSchema] = {
    var solrList: List[SlorSchema] = new ArrayList[SlorSchema]
    var solrClient: SolrClient = null
    try {
      solrClient = new HttpSolrClient("http://192.168.37.11:8983/solr/solr-ylzx")
      val params: ModifiableSolrParams = getSolrParamsById
      val rsp: QueryResponse = solrClient.query(params, SolrRequest.METHOD.POST)
      if (rsp != null && rsp.getResults != null && rsp.getResults.size > 0) {
        solrList = rsp.getBeans(classOf[SlorSchema])
      }
    }
    catch {
      case e: Exception => {
        e.printStackTrace
      }
    } finally {
      if (solrClient != null) {
        try {
          solrClient.close
        }
        catch {
          case e2: Exception => {
            System.out.println("Solr客户端关闭失败！")
            e2.printStackTrace
          }
        }
      }
    }
    return solrList
  }

  def getSolrParamsById: ModifiableSolrParams = {
    val params: ModifiableSolrParams = new ModifiableSolrParams
    val filterQuery: SolrQuery = new SolrQuery
    val fromDoc: Int = 0
    val rows: Int = 53127
    filterQuery.setQuery("*:*")
    filterQuery.addFilterQuery("title:*")
    filterQuery.setStart(fromDoc)
    filterQuery.setRows(rows)
    params.add(filterQuery)
    return params
  }

  @throws(classOf[IOException])
  def main(args: Array[String]) {
    val fos: FileOutputStream = new FileOutputStream("D:\\result.txt")
    val osw: OutputStreamWriter = new OutputStreamWriter(fos)
    val contentList: List[Content] = GetDataService.getData

    var i: Int = 0
    while (i < contentList.size) {
      osw.write(contentList.get(i).getId + "," + contentList.get(i).getLastModified + "\r\n")
      i += 1
    }
    //osw.flush();
    System.out.println(contentList.size)
    osw.close


  }
}
