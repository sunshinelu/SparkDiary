package com.evayInfo.Inglory.SparkDiary.database.solr;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sunlu on 17/6/21.
 */
public class GetDataService {


    public static List<Content> getData() {
        List<Content> contentList = new ArrayList<Content>();
        SolrClient solrClient = null;
        try {
            solrClient = new HttpSolrClient("http://192.168.37.11:8983/solr/solr-ylzx");
            //组装solr的检索条件
            ModifiableSolrParams params = getSolrParamsById();
            QueryResponse rsp = solrClient.query(params, SolrRequest.METHOD.POST);
            if (rsp != null && rsp.getResults() != null && rsp.getResults().size() > 0) {
                contentList = rsp.getBeans(Content.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (solrClient != null) {
                try {
                    solrClient.close();
                } catch (Exception e2) {
                    System.out.println("Solr客户端关闭失败！");
                    e2.printStackTrace();
                }
            }
        }
        return contentList;
    }

    public static ModifiableSolrParams getSolrParamsById() {
        ModifiableSolrParams params = new ModifiableSolrParams();
        SolrQuery filterQuery = new SolrQuery();
        int fromDoc = 0;
        int rows = 100000;
       // filterQuery.addFilterQuery("title:*");//查询标题不为空
        filterQuery.setStart(fromDoc);
//        filterQuery.addSort("lastModified", ORDER.desc);
        filterQuery.setRows(rows);
        params.add(filterQuery);
        return params;
    }

    public static void main(String[] args) throws IOException {

        List<Content> contentList = GetDataService.getData();
        System.out.println("=================");
        System.out.println(contentList.size());
        for (int i = 0; i <= 10; i++) {
            // System.out.println(contentList.get(i).getId()+"---"+contentList.get(i).getLastModified());
        }
    }

}
