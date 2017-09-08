package com.evayInfo.Inglory.SparkDiary.database.solr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/***
 * 从solr里读取标签是商务的数据写入hbase
 *
 * @author 孙伟
 */
public class Solr_hbase_shangwu {

    private static Configuration conf = null;

    static {
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.master", "192.168.37.21:60000");
        //HBASE_CONFIG.set("hbase.master", "192.168.37.10:60000");
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.37.21,192.168.37.22,192.168.37.23");//
        //HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.37.10,192.168.37.11");
        //HBASE_CONFIG.set("hbase.zookeeper.quorum", "172.16.96.21,172.16.96.22");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        conf = HBaseConfiguration.create(HBASE_CONFIG);
    }

    private static final String DEFAULT_URL = "http://192.168.37.11:8983/solr/solr-ylzx";
    //private static final String DEFAULT_URL = "http://10.20.5.115:8983/solr/solr-ylzx";
    private static HttpSolrServer server;

    public static void init() {
        server = new HttpSolrServer(DEFAULT_URL);
        server.setMaxRetries(3); // defaults to 0. > 1 not recommended.
        server.setConnectionTimeout(8000); // 5 seconds to establish TCP
        server.setSoTimeout(8000); // socket read timeout
        server.setDefaultMaxConnectionsPerHost(100);
        server.setMaxTotalConnections(100);
        server.setFollowRedirects(false); // defaults to false
        server.setAllowCompression(true);
        server.getParser().getVersion();
    }

    public static void querySimple() throws Exception {
        HTable table = new HTable(conf, "yilan-total_webpage");
        // 这是一个简单查询
        //String urlfilter="cistc";
        //  DateFormat format = new SimpleDateFormat("yyyy-MM-dd") ;
        //  String dayTwoBefore=format.format(new Date(new Date().getTime() - (long)2 * 24 * 60 * 60 * 1000));
        // System.out.println("两天前的日期：" + format.format(new Date(new Date().getTime() - (long)2 * 24 * 60 * 60 * 1000)));
        //  dayTwoBefore=dayTwoBefore+"T00:00:00Z";

        ModifiableSolrParams params = new ModifiableSolrParams();
        // String sj="lastModified :[2017-06-14T16:00:00.294Z TO 2017-06-15T02:50:00.294Z]";
        //  String sj="lastModified :[2017-04-23T00:00:00.344Z TO *]";
        //  sj="lastModified :["+dayTwoBefore+" TO *]";
        params.set("q", "manuallabel:\"商务\"");
        // params.set("q", "id:3f8ed9a1-541b-4fb1-97f8-d2987935ab3a1");
        params.set("wt", "xml");
        params.set("indent", "true");
        // params.set("q.op", "");
        // params.set("start", 0);
        params.set("rows", 3800);
        //params.set("fq","%E5%95%86%E5%8A%A1");
        // params.set("fl", "");
        // params.set("version", "1");
        try {
            QueryResponse response = server.query(params);
            //response.setResponse(arg0);
            SolrDocumentList list = response.getResults();
            System.out.println("总计：" + list.getNumFound() + "条，本批次:" + list.size() + "条");
            int k = 0;
            for (int i = 0; i < list.size(); i++) {
                SolrDocument doc = list.get(i);
                Map map = new HashMap();

                String rowKey = doc.get("id").toString();
                int j = getOneRecord("yilan-total_webpage", rowKey, table);
                if (j > 0) {
                    System.out.println(rowKey + "已存在");
                    continue;

                } else {

                    map.put("rowKey", rowKey);
                    map.put("t", doc.get("title").toString());
                    map.put("c", doc.get("content").toString());
                    map.put("appc", doc.get("appc").toString());
                    map.put("websitename", doc.get("websitename").toString());
                    map.put("manuallabel", doc.get("manuallabel").toString());
                    map.put("column_name", doc.get("column_name").toString());
                    map.put("websitelb", doc.get("websitelb") != null ? doc.get("websitelb").toString() : "");
                    map.put("xzqhname", doc.get("xzqhname") != null ? doc.get("xzqhname").toString() : "");
                    map.put("websitejb", doc.get("websitejb") != null ? doc.get("websitejb").toString() : "");
                    map.put("bas", doc.get("url") != null ? doc.get("url").toString() : "");//f

                    map.put("column_type", doc.get("column_type") != null ? doc.get("column_type").toString() : "");

                    map.put("websiteid", doc.get("websiteid") != null ? doc.get("websiteid").toString() : "");
                    map.put("column_id", doc.get("column_id") != null ? doc.get("column_id").toString() : "");
                    map.put("websiteid", doc.get("websiteid") != null ? doc.get("websiteid").toString() : "");
                    map.put("sfzs", doc.get("sfzs") != null ? doc.get("sfzs").toString() : "");
                    map.put("sfcj", doc.get("sfcj") != null ? doc.get("sfcj").toString() : "");
                    map.put("imgCount", doc.get("imgCount") != null ? doc.get("imgCount").toString() : "");
                    map.put("imgUrl", doc.get("imgUrl") != null ? doc.get("imgUrl").toString() : "");
                    map.put("bigImgPath", doc.get("bigImgPath") != null ? doc.get("bigImgPath").toString() : "");

                    map.put("smallImgPath", doc.get("smallImgPath") != null ? doc.get("smallImgPath").toString() : "");
                    map.put("fjCount", doc.get("fjCount") != null ? doc.get("fjCount").toString() : "");
                    map.put("fjPath", doc.get("fjPath") != null ? doc.get("fjPath").toString() : "");
                    map.put("fjTitle", doc.get("fjTitle") != null ? doc.get("fjTitle").toString() : "");
                    map.put("websitebq", doc.get("websitebq") != null ? doc.get("websitebq").toString() : "");
                    Date date = (Date) doc.get("lastModified");

                    String sj1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
                    map.put("mod", sj1);

                    insertHbase(map, table);
                    System.out.println("插入了第" + i + "条");
                    k++;
                }


            }
            System.out.println("本次统计数量：" + k);
        } catch (SolrServerException e) {
            e.printStackTrace();
        }

        System.out.println("--------------------------");
    }


    public static void insertHbase(Map map, HTable table) throws IOException, ParserConfigurationException, SAXException {


        // HTable table = new HTable(conf, "swtest1_webpage");

        String rowKey = map.get("rowKey").toString();
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("websitejb"), Bytes.toBytes(map.get("websitejb").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("websiteurl"), Bytes.toBytes(""));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("websiteid"), Bytes.toBytes(map.get("websiteid").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("websitelb"), Bytes.toBytes(map.get("websitelb").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("websitezffl"), Bytes.toBytes(""));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("websitebq"), Bytes.toBytes(map.get("websitebq").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("textexp"), Bytes.toBytes(""));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("sfzs"), Bytes.toBytes(map.get("sfzs").toString()));
        //新增是否采集字段，分辨内容相同时，保证栏目里也有此数据
        put.add(Bytes.toBytes("p"), Bytes.toBytes("sfcj"), Bytes.toBytes(map.get("sfcj").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("column_type"), Bytes.toBytes(map.get("column_type").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("xzqhname"), Bytes.toBytes(map.get("xzqhname").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("xzqhcode"), Bytes.toBytes(""));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("t"), Bytes.toBytes(map.get("t").toString()));
//			put.add(Bytes.toBytes("f"), Bytes.toBytes("mod"), Long.toString(DateUtils.StrToLong(map.get("SEED_DATE").toString(), "yyyy-MM-dd HH:mm:ss")).getBytes());
        put.add(Bytes.toBytes("f"), Bytes.toBytes("mod"), Bytes.toBytes(DateUtils.StrToLong(map.get("mod").toString(), "yyyy-MM-dd HH:mm:ss")));
        // put.add(Bytes.toBytes("f"),Bytes.toBytes("webmod"),Bytes.toBytes(DateUtils.StrToLong(map.get("WEB_DATE").toString(),"yyyy-MM-dd HH:mm:ss")));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("bas"), Bytes.toBytes(map.get("bas").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("c"), Bytes.toBytes(map.get("c").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("websitename"), Bytes.toBytes(map.get("websitename").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("column_name"), Bytes.toBytes(map.get("column_name").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("column_id"), Bytes.toBytes(map.get("column_id").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"), Bytes.toBytes(map.get("manuallabel").toString()));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("isindex"), Bytes.toBytes("1"));

        put.add(Bytes.toBytes("p"), Bytes.toBytes("imgCount"), Bytes.toBytes(map.get("imgCount").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("bigImgPath"), Bytes.toBytes(map.get("bigImgPath").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("smallImgPath"), Bytes.toBytes(map.get("smallImgPath").toString()));

        put.add(Bytes.toBytes("p"), Bytes.toBytes("accessoryCount"), Bytes.toBytes(map.get("fjCount").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("accessoryPath"), Bytes.toBytes(map.get("fjPath").toString()));
        put.add(Bytes.toBytes("p"), Bytes.toBytes("accessoryTitle"), Bytes.toBytes(map.get("fjTitle").toString()));
        table.put(put);

    }

    public static int getOneRecord(String tableName, String rowKey, HTable table) throws IOException, ParserConfigurationException, SAXException {
        //tableName="";
        int i = 0;
        //HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());

        org.apache.hadoop.hbase.client.Result rs = table.get(get);

        i = rs.size();
        return i;
    }


    public static void main(String[] args) throws Exception {
        init();
        //getOneRecord("yilan-total_webpage","1cbff174-2194-4556-90af-a11e4656c2d4");
        querySimple();
    }
}
