package com.evayInfo.Inglory.SparkDiary.database.solr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.stereotype.Service;

@Service
public class AddIndex {

    // 声明hbase静态配置
    private static Configuration conf = null;

    public static void getHbaseCon() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.37.21,192.168.37.22,192.168.37.23");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "192.168.37.22:60000", "192.168.37.23:60000");

        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");
        conf.addResource("hbase-site.xml");
    }

    //获取HBASE表中的所有结果集
    public static ResultScanner getAllRows(String tableName) throws IOException {
        getHbaseCon();
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        return results;
    }

    //输出HBASE表中的结果
    public static void getRecommendResult(ResultScanner results) {

        for (Result result : results) {
            String label = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("label")).getValue());
            String level = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("level")).getValue());
            String relateUrl = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("relateUrl")).getValue());
            String simsScore = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("simsScore")).getValue());
            String time = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("time")).getValue());
            String title = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("title")).getValue());
            String url = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("url")).getValue());
            System.out.println("label:" + label + " title:" + title + " level: " + level + " relateUrl:" + relateUrl + " simsScore:" + simsScore + " time:" + time + " url:" + url);
        }
    }


    public static void addIndex(ResultScanner results) {

        HttpSolrServer server;
        String solrUrl_similarArticleRec = "http://192.168.37.11:8983/solr/solr-yilan-similarArticleRec";
        server = new HttpSolrServer(solrUrl_similarArticleRec);
        try {
            for (Result result : results) {
                SolrInputDocument document = new SolrInputDocument();
                String label = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("label")).getValue());
                String level = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("level")).getValue());
                String relateUrl = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("relateUrl")).getValue());
                String simsScore = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("simsScore")).getValue());
                String time = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("time")).getValue());
                String title = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("title")).getValue());
                String url = Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("url")).getValue());
                document.addField("id", url);
                document.addField("label", label);
                document.addField("level", level);
                document.addField("relateUrl", relateUrl);
                document.addField("simsScore", simsScore);
                document.addField("time", time);
                document.addField("title", title);
                document.addField("url", url);
                UpdateResponse response = server.add(document);
                server.commit();
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {

        String tableName = "t_userProfileV1";
        ResultScanner results = getAllRows(tableName);
        addIndex(results);
        //getRecommendResult(results);

    }
    /*	定时器（计划任务）示例
     @Scheduled(cron="0/10 * * * * ? ")
	public void noTranScheduleTests(){
		System.out.println("schedule works!"+System.currentTimeMillis());
	}*/
}

