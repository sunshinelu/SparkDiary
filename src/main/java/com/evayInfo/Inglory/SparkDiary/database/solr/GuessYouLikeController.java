package com.evayInfo.Inglory.SparkDiary.database.solr;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.sdyy.common.service.ConfigService;
import com.sdyy.recommendSys.recommendResult.common.CyptoUtils;

@Controller
@RequestMapping("/guessYouLike")
public class GuessYouLikeController {
	
	/*
	 * 猜你喜欢推荐结果(若猜你喜欢结果集索引库中存在则根据用户id从结果集中取,
	 * 若结果集中不存在则取最热门的文章进行推荐)
	 */
	@RequestMapping(value = "/recResult", method = RequestMethod.GET,produces = {"application/json;charset=UTF-8"})
	@ResponseBody 
	public String getGuessYouLikeResult(String userIdEncoded) {
		String userId = CyptoUtils.decode(userIdEncoded);
		String solrUrl_guessYouLikeRec = ConfigService.getConfig(ConfigService.SOLRURL_GUESSYOULIKEREC);
		//String solrUrl_guessYouLikeRec = "http://192.168.37.11:8983/solr/solr-yilan-guessYouLikeRec";
		SolrClient solrClient = null;
    	QueryResponse rsp = null; 
    	String guessYouLikeRecResult = null;
    	StringBuffer  guessYouLike_InResult= new StringBuffer();
        try {
        	solrClient = new HttpSolrClient(solrUrl_guessYouLikeRec);
        	ModifiableSolrParams params = new ModifiableSolrParams();
    		SolrQuery solrQuery = new SolrQuery();
    		solrQuery.setQuery("userID:\"" + userId + "\" ");
    		solrQuery.addFilterQuery("-title:\"\"");//查询标题不为空串
    		solrQuery.setStart(0);
    		solrQuery.setRows(5);
    		solrQuery.setSort("rn", ORDER.asc);
    		params.add(solrQuery);
            rsp = solrClient.query(params);
            if (rsp != null && rsp.getResults() != null && rsp.getResults().size() > 0) {
            	SolrDocumentList  documentList = rsp.getResults();
            	guessYouLike_InResult.append("[");
            	for(int i = 0;i < documentList.size();i++){
            		String simsID = "";
            		String manuallabel = "";
            		String title = "";
            		String lastModified = "";
            		if(null != documentList.get(i).get("articleId")){
            			simsID=documentList.get(i).get("articleId").toString();
        			}
            		if(null != documentList.get(i).get("manuallabel")){
            			manuallabel=documentList.get(i).get("manuallabel").toString();
        			}
            		if(null != documentList.get(i).get("title")){
            			title=documentList.get(i).get("title").toString();
        			}
            		if(null != documentList.get(i).get("mod")){
            			lastModified=documentList.get(i).get("mod").toString();
        			}
            		if(i == documentList.size()-1){
            			guessYouLike_InResult.append("{\"simsID\":\""+CyptoUtils.encode(simsID)+"\",\"title\":\""+title +"\",\"manuallabel\":\""+manuallabel + "\",\"lastModified\":\""+lastModified+"\"}]");
        			}else{
        				guessYouLike_InResult.append("{\"simsID\":\""+CyptoUtils.encode(simsID)+"\",\"title\":\""+title +"\",\"manuallabel\":\""+manuallabel + "\",\"lastModified\":\""+lastModified+"\"},");
        			}
        		}
            	guessYouLikeRecResult = guessYouLike_InResult.toString();
			}
		} catch (SolrServerException e) {
			throw new RuntimeException(e.getMessage(), e);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			if(null != solrClient){
				try {
					solrClient.close();
				} catch (IOException e) {}
			}
		}
        
        return guessYouLikeRecResult;
	  }
	
	public static void main(String[] args){
		String userId = "175786f8-1e74-4d6c-94e9-366cf1649721";
		//String userIdEncode = "DSWJ602D1ECB972F21C04D85D6A633C34CC44D672F67393837D8554059D61D17A5743B7542262D9F3D25ID5Y";
		String userIdEncode = CyptoUtils.encode(userId);
		System.out.println(userIdEncode);
	}
}
