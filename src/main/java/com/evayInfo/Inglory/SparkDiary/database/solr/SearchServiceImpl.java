package com.evayInfo.Inglory.SparkDiary.database.solr;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.sdyy.common.dao.IBaseDao;
import com.sdyy.common.utils.CyptoUtils;
import com.sdyy.common.utils.GetLocalFromIP;
import com.sdyy.common.utils.GetProperties;
import com.sdyy.common.utils.StringUtils;
import com.sdyy.yeeso.nrgl.solrArticle.service.ISolrArticleService;
import com.sdyy.ylzx.index.search.entity.ContentForSolrj;
import com.sdyy.ylzx.index.search.entity.ContentForSolrjDetail;
import com.sdyy.ylzx.yhzx.grzx.service.ISubService;

@Service
public class SearchServiceImpl implements ISearchService{

	@Autowired
	private IBaseDao dao;
	@Autowired
	private ISubService subService;
	@Autowired
	private ISolrArticleService solrArticleService; 
	private static String SOLR_URL = GetProperties.getConfig("ylzx.solr.url");
	private static String SOLR_CORE = GetProperties.getConfig("ylzx.solr.core");
	
	public static TimeZone UTC = TimeZone.getTimeZone("UTC");

	/**
	 * 检索页面
	 * 李灿
	 * 2017年2月16日10:50:33
	 */
	public Map<String, Object> search(Map<String, Object> paraMap){
		Map<String, Object>  resultMap = new HashMap<String, Object>();  //返回结果Map
		long totalDocs = 0;   //返回条数
		//返回的检索的内容接口
		List<ContentForSolrj> contentList = new ArrayList<ContentForSolrj>();;
		SolrClient solrClient = null;
		try {
			solrClient = new HttpSolrClient(SOLR_URL + SOLR_CORE);
			//组装solr的检索条件
			ModifiableSolrParams params = this.getSolrParams(paraMap);
			QueryResponse rsp = solrClient.query(params,METHOD.POST);
			if (rsp != null && rsp.getResults() != null && rsp.getResults().size() > 0) {
				totalDocs = rsp.getResults().getNumFound();			
				contentList = rsp.getBeans(ContentForSolrj.class);
			}
			
			contentList = hitsAndLikeslist(contentList);
			//对ID进行加密处理，防止出现在id中存在的?或者&等特殊字符
			for (ContentForSolrj content : contentList) {
				content.setId(CyptoUtils.encode(content.getId()));
				//判断正文内容的长度是不是大于100，如果大于100，怎截取前100个字符传至前台，用于节省流量
				content.setContent((content!=null && content.getContent()!=null)?(content.getContent().length()>100?content.getContent().substring(0, 100):content.getContent()):"");
				if(content.getManuallabel().length()>0 && content.getManuallabel().contains("招投标")){
					content.setWebsitename("某招投标网站");
				}
			}
			resultMap.put("contentForSolr", contentList);
			resultMap.put("totalDocs", totalDocs);
			resultMap.put("manuallabel", paraMap.get("manuallabel"));
			resultMap.put("xzqhname", paraMap.get("xzqhname"));
		} catch (Exception e) {
			System.out.println("检索出现异常！");
			e.printStackTrace();
		} finally{
			if (solrClient != null) {
				try {
					solrClient.close();
				} catch (Exception e2) {
					System.out.println("Solr客户端关闭失败！");
					e2.printStackTrace();
				}
			}
		}
		return resultMap;
	}
	
	/**
	 * 获取我订阅的内容
	 */
	public Map<String, Object> searchByMySub(Map<String, Object> paraMap){
		Map<String, Object> resultMap = new HashMap<String, Object>();
		long totalDocs = 0;   //返回条数
		//返回的检索的内容接口
		List<ContentForSolrj> contentList = new ArrayList<ContentForSolrj>();
		SolrClient solrClient = null;
		try {
			solrClient = new HttpSolrClient(SOLR_URL + SOLR_CORE);
			//组装solr的检索条件
			ModifiableSolrParams params = this.getSolrParamsByMySub(paraMap);
			QueryResponse rsp = solrClient.query(params,METHOD.POST);
			if (rsp != null && rsp.getResults() != null && rsp.getResults().size() > 0) {
				totalDocs = rsp.getResults().getNumFound();			
				contentList = rsp.getBeans(ContentForSolrj.class);
			}
			
			contentList = hitsAndLikeslist(contentList);
			//对ID进行加密处理，防止出现在id中存在的?或者&等特殊字符
			for (ContentForSolrj content : contentList) {
				content.setId(CyptoUtils.encode(content.getId()));
				//判断正文内容的长度是不是大于100，如果大于100，怎截取前100个字符传至前台，用于节省流量
				content.setContent((content!= null && content.getContent()!=null)?(content.getContent().length()>100?content.getContent().substring(0, 100):content.getContent()):"");
				if(content.getManuallabel().length()>0 && content.getManuallabel().contains("招投标")){
					content.setWebsitename("某招投标网站");
				}
			}
			resultMap.put("contentForSolr", contentList);
			resultMap.put("totalDocs", totalDocs);
		} catch (Exception e) {
			System.out.println("检索出现异常！");
			e.printStackTrace();
		} finally{
			if (solrClient != null) {
				try {
					solrClient.close();
				} catch (Exception e2) {
					System.out.println("Solr客户端关闭失败！");
					e2.printStackTrace();
				}
			}
		}
		return resultMap;
	}
	
	/**
	 * 根据操作员ID查询
	 */
	public List<ContentForSolrj> searchByOperatorId(String operatorId){
		//返回的检索的内容接口
		List<ContentForSolrj> contentList = new ArrayList<ContentForSolrj>();
		SolrClient solrClient = null;
		try {
			solrClient = new HttpSolrClient(SOLR_URL + SOLR_CORE);
			//组装solr的检索条件
			ModifiableSolrParams params = this.getSolrParamsByMySub(operatorId);
			QueryResponse rsp = solrClient.query(params,METHOD.POST);
			if (rsp != null && rsp.getResults() != null && rsp.getResults().size() > 0) {
				contentList = rsp.getBeans(ContentForSolrj.class);
			}
			
			//contentList = hitsAndLikeslist(contentList);
			//对ID进行加密处理，防止出现在id中存在的?或者&等特殊字符
			for (ContentForSolrj content : contentList) {
				content.setId(CyptoUtils.encode(content.getId()));
				//判断正文内容的长度是不是大于100，如果大于100，怎截取前100个字符传至前台，用于节省流量
				content.setContent((content!=null && content.getContent() != null)?(content.getContent().length()>100?content.getContent().substring(0, 100):content.getContent()):"");
				if(content.getManuallabel().length()>0 && content.getManuallabel().contains("招投标")){
					content.setWebsitename("某招投标网站");
				}
			}
		} catch (Exception e) {
			System.out.println("检索出现异常！");
			e.printStackTrace();
		} finally{
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
	
	/**
	 * 根据关键词检索
	 */
	public Map<String, Object> searchByKeyword(Map<String, Object> paraMap){
		Map<String, Object>  resultMap = new HashMap<String, Object>();  //返回结果Map
		long totalDocs = 0;   //返回条数
		//返回的检索的内容接口
		List<ContentForSolrj> contentList = new ArrayList<ContentForSolrj>();;
		SolrClient solrClient = null;
		try {
			solrClient = new HttpSolrClient(SOLR_URL + SOLR_CORE);
			//组装solr的检索条件
			ModifiableSolrParams params = this.getSolrParamsByKeyword(paraMap);
			QueryResponse rsp = solrClient.query(params,METHOD.POST);
			if (rsp != null && rsp.getResults() != null && rsp.getResults().size() > 0) {
				totalDocs = rsp.getResults().getNumFound();			
				contentList = rsp.getBeans(ContentForSolrj.class);
			}
			
			contentList = hitsAndLikeslist(contentList);
			//对ID进行加密处理，防止出现在id中存在的?或者&等特殊字符
			for (ContentForSolrj content : contentList) {
				content.setId(CyptoUtils.encode(content.getId()));
				//判断正文内容的长度是不是大于100，如果大于100，怎截取前100个字符传至前台，用于节省流量
				content.setContent((content!=null && content.getContent() != null)?(content.getContent().length()>100?content.getContent().substring(0, 100):content.getContent()):"");
				if(content.getManuallabel() != null && content.getManuallabel().length()>0 && content.getManuallabel().contains("招投标")){
					content.setWebsitename("某招投标网站");
				}
			}
			List<String> keywords = new ArrayList<String>();
			/*使用分词时放开注释
			 * Result result= ToAnalysis.parse(paraMap.get("keyword").toString());
			for (Term term : result) {
				if (term.getName().trim().length()>0) {
					keywords.add(term.getName());
				}
			}*/
			keywords.add(paraMap.get("keyword").toString());
			resultMap.put("contentForSolr", contentList);
			resultMap.put("totalDocs", totalDocs);
			resultMap.put("Keywords", keywords);
		} catch (Exception e) {
			System.out.println("检索出现异常！");
			e.printStackTrace();
		} finally{
			if (solrClient != null) {
				try {
					solrClient.close();
				} catch (Exception e2) {
					System.out.println("Solr客户端关闭失败！");
					e2.printStackTrace();
				}
			}
		}
		return resultMap;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> getContentById(Map<String, Object> paraMap){
		Map<String, Object>  resultMap = new HashMap<String, Object>();
		//返回的检索的内容接口
		List<ContentForSolrjDetail> contentList = new ArrayList<ContentForSolrjDetail>();
		SolrClient solrClient = null;
		Map<String, Object> mapHits = new HashMap<String, Object>();
		try {
			solrClient = new HttpSolrClient(SOLR_URL + SOLR_CORE);
			//组装solr的检索条件
			ModifiableSolrParams params = this.getSolrParamsById(paraMap);
			QueryResponse rsp = solrClient.query(params,METHOD.POST);
			if (rsp != null && rsp.getResults() != null && rsp.getResults().size() > 0) {
				contentList = rsp.getBeans(ContentForSolrjDetail.class);
			}
			ContentForSolrjDetail content = contentList.get(0);
			content.setUrl(CyptoUtils.encode(content.getUrl()));
			content.setId(CyptoUtils.encode(content.getId()));
			content.setMarks(content.getManuallabel());
			mapHits =  solrArticleService.get(CyptoUtils.decode(paraMap.get("id").toString()));
			if(null == mapHits){//没有记录则新增
				Map<String, Object> mapRecord = new HashMap<String, Object>();
				mapRecord.put("ARTICLE_ID", CyptoUtils.decode(paraMap.get("id").toString()));
				mapRecord.put("HITS", 1);
				mapRecord.put("TITLE", content.getTitle().get(0));
				solrArticleService.insert(mapRecord);
				content.setLikes(0);
				content.setComments(0);
			}else{
				mapHits.put("HITS", mapHits.containsKey("HITS")?(mapHits.get("HITS").toString()==null ? 0 :Integer.parseInt(mapHits.get("HITS").toString()) + 1):1);
				solrArticleService.update(mapHits);
				content.setLikes(mapHits.get("LIKES")==null ? 0 :Integer.parseInt(mapHits.get("LIKES").toString()));
				content.setComments(mapHits.get("COMMENTS")==null ? 0 :Integer.parseInt(mapHits.get("COMMENTS").toString()));
			}
			resultMap.put("contentForSolr", content);
			resultMap.put("dqwz", "中国".equals(paraMap.get("dqwz"))?"中央":paraMap.get("dqwz"));
			
		} catch (Exception e) {
			System.out.println("检索出现异常！");
			e.printStackTrace();
		} finally{
			if (solrClient != null) {
				try {
					solrClient.close();
				} catch (Exception e2) {
					System.out.println("Solr客户端关闭失败！");
					e2.printStackTrace();
				}
			}
		}
		return resultMap;
	}
	
	/**
	 * 组装查询条件
	 *@author:李灿--lic@sdas.org
	 *@Version : V1.0
	 *@ModifiedBy 李灿
	 *@Copyright:山东亿云信息技术有限公司--天枢大数据团队
	 *@date 2017年2月16日 上午10:27:57
	 */
	@SuppressWarnings("unchecked")
	private ModifiableSolrParams getSolrParams(Map<String, Object> paraMap){
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrQuery filterQuery = new SolrQuery();
		int fromDoc = 0;
		int rows = 20;
		if (paraMap.containsKey("sType") && "index".equals(paraMap.get("sType").toString())) {//如果是检索首页
			fromDoc = 0;
			rows = 6;
			if (paraMap.containsKey("manuallabel") && !StringUtils.isEmpty(paraMap.get("manuallabel"))) {
				filterQuery.setQuery("manuallabel:\""+paraMap.get("manuallabel").toString()+"\"");
			}
			if(paraMap.containsKey("xzqhname") && !StringUtils.isEmpty(paraMap.get("xzqhname"))){
				if ("本地".equals(paraMap.get("xzqhname"))) {
					String local[] = new String[2];
					//1.去数据库查询IP所属地区
					Map<String, Object> map = new HashMap<String,Object>();
					if (paraMap.containsKey("IP")) {
						paraMap.put("IPADRESS", GetLocalFromIP.ipToLong(paraMap.get("IP").toString()));
						map = (Map<String, Object>)dao.get("SearchMapper.getAdressByIP", paraMap);
						local[0] = (map != null && map.containsKey("PROVINCE"))?map.get("PROVINCE").toString():"";
						local[1] = (map != null && map.containsKey("CITY"))?map.get("CITY").toString():"";
						//2.去淘宝接口获取IP所属地区并将数据保存到数据库中
						if (local.length==0 || (local[0]=="" && local[1]=="")) {
							local = GetLocalFromIP.getAddressFromIP(paraMap.get("IP").toString()).split(":");
							if(local.length != 0){
								paraMap.put("PROVINCE", local[0]);
								paraMap.put("CITY", local[1]);
								dao.insert_("SearchMapper.insert", paraMap);
							}
						}
					}
					//3.都没取到，默认济南
					if (local.length==0) {
						local = new String[2];
						local[0] = "山东省"; 
						local[1] = "济南市"; 
					}
					filterQuery.setQuery("xzqhname:\""+local[0].replace("省", "")+"\" || xzqhname:\""+local[1].replace("市", "")+"\"");
				}else {
					filterQuery.setQuery("xzqhname:\""+paraMap.get("xzqhname").toString()+"\"");
				}
			}
			if(paraMap.containsKey("websitelb") && !StringUtils.isEmpty(paraMap.get("websitelb"))){
				filterQuery.setQuery("websitelb:\""+paraMap.get("websitelb").toString()+"\"");
			}
			if(paraMap.containsKey("websitejb") && !StringUtils.isEmpty(paraMap.get("websitejb"))){
				filterQuery.setQuery("websitejb:\""+paraMap.get("websitejb").toString()+"\"");
			}
			filterQuery.addFilterQuery("title:*");//查询标题不为空
		}else{//详情列表
			if (paraMap.containsKey("manuallabel") && !StringUtils.isEmpty(paraMap.get("manuallabel"))) {
				filterQuery.setQuery("manuallabel:\""+paraMap.get("manuallabel").toString()+"\"");
			}
			if (paraMap.containsKey("cuPage") && Integer.parseInt(paraMap.get("cuPage").toString()) > 1) {
				fromDoc = rows * (Integer.parseInt(paraMap.get("cuPage").toString())-1);
			}
			if(paraMap.containsKey("xzqhname") && !StringUtils.isEmpty(paraMap.get("xzqhname"))){
				if ("本地".equals(paraMap.get("xzqhname"))) {
					String local[] = new String[2];
					//1.去数据库查询IP所属地区
					Map<String, Object> map = new HashMap<String,Object>();
					if (paraMap.containsKey("IP")) {
						paraMap.put("IPADRESS", GetLocalFromIP.ipToLong(paraMap.get("IP").toString()));
						map = (Map<String, Object>)dao.get("SearchMapper.getAdressByIP", paraMap);
						local[0] = (map != null && map.containsKey("PROVINCE"))?map.get("PROVINCE").toString():"";
						local[1] = (map != null && map.containsKey("CITY"))?map.get("CITY").toString():"";
						//2.去淘宝接口获取IP所属地区并将数据保存到数据库中
						if (local.length==0 || (local[0]=="" && local[1]=="")) {
							local = GetLocalFromIP.getAddressFromIP(paraMap.get("IP").toString()).split(":");
							if(local.length != 0){
								paraMap.put("PROVINCE", local[0]);
								paraMap.put("CITY", local[1]);
								dao.insert_("SearchMapper.insert", paraMap);
							}
						}
					}
					//3.都没取到，默认济南
					if (local.length==0) {
						local = new String[2];
						local[0] = "山东省"; 
						local[1] = "济南市"; 
					}
					filterQuery.setQuery("xzqhname:\""+local[0].replace("省", "")+"\" || xzqhname:\""+local[1].replace("市", "")+"\"");
				}else {
					filterQuery.setQuery("xzqhname:\""+paraMap.get("xzqhname").toString()+"\"");
				}
			}
			if(paraMap.containsKey("websitelb") && !StringUtils.isEmpty(paraMap.get("websitelb"))){
				filterQuery.setQuery("websitelb:\""+paraMap.get("websitelb").toString()+"\"");
			}
			if(paraMap.containsKey("websitejb") && !StringUtils.isEmpty(paraMap.get("websitejb"))){
				filterQuery.setQuery("websitejb:\""+paraMap.get("websitejb").toString()+"\"");
			}
			filterQuery.addFilterQuery("title:*");//查询标题不为空
		}
		if((paraMap.containsKey("manuallabel") && (!"招投标".equals(paraMap.get("manuallabel")))) || (!paraMap.containsKey("manuallabel"))){
			filterQuery.addFilterQuery("-manuallabel:\"招投标\"");
		}
		if(paraMap.containsKey("xzqhname") && ("中国".equals(paraMap.get("xzqhname"))||"中央".equals(paraMap.get("xzqhname")))){
			filterQuery.addFilterQuery("websitejb:*");
		}
		filterQuery.addFilterQuery("content:*");
		filterQuery.setStart(fromDoc);
		filterQuery.addSort("lastModified", ORDER.desc);
		filterQuery.setRows(rows);
		params.add(filterQuery);
		return params;
	}
	
	/**
	 * 根据我的订阅来组装查询条件
	 *@author:李灿--lic@sdas.org
	 *@Version : V1.0
	 *@ModifiedBy 李灿
	 *@Copyright:山东亿云信息技术有限公司--天枢大数据团队
	 *@date 2017年3月18日 下午5:13:13
	 */
	private ModifiableSolrParams getSolrParamsByMySub(Map<String, Object> paraMap){
		//获取当前用户的所有订阅列表
		List<Map<String, Object>> mySubList = subService.queryMySubList(paraMap);
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrQuery filterQuery = new SolrQuery();
		int fromDoc = 0;
		int rows = 20;
		if (paraMap.containsKey("cuPage") && Integer.parseInt(paraMap.get("cuPage").toString()) > 1) {
			fromDoc = rows * (Integer.parseInt(paraMap.get("cuPage").toString())-1);
		}
		filterQuery.setQuery("*:*");
		//根据订阅的内容组装查询条件组装查询
		StringBuffer sb = new StringBuffer();
		for (Map<String, Object> map : mySubList) {
			sb.append("(");
			if (map.containsKey("WEBSITE_NAME") && map.get("WEBSITE_NAME").toString().length()>0) {
				sb.append("websitename:\""+map.get("WEBSITE_NAME").toString()+"\"");
				if (map.containsKey("COLUMN_NAME") && map.get("COLUMN_NAME").toString().length()>0) {
					sb.append("AND column_name:\""+map.get("COLUMN_NAME").toString()+"\"");
				}
			}
			sb.append(") OR ");
		}
		String fileterQuery = sb.toString();
		if (fileterQuery.length()>3) {
			fileterQuery = fileterQuery.substring(0, fileterQuery.length()-3);
		}
		
		if(paraMap.containsKey("keyword") && !"".equals(paraMap.get("keyword").toString())){
//			filterQuery.addFilterQuery("title:\""+paraMap.get("keywore").toString()+"\" OR appc:");
			filterQuery.addFilterQuery("title:\""+paraMap.get("keyword").toString()+"\" || appc:\""+paraMap.get("keyword").toString()+"\"");
		}
		
		if (fileterQuery.length()>0) {
			filterQuery.addFilterQuery(fileterQuery);
		}
		filterQuery.addFilterQuery("title:*");//查询标题不为空
		filterQuery.addFilterQuery("content:*");
		filterQuery.setStart(fromDoc);
		filterQuery.addSort("lastModified", ORDER.desc);
		filterQuery.setRows(rows);
		params.add(filterQuery);
		return params;
	}
	
	/**
	 *根据操作员ID组装查询条件
	 *@author:李灿--lic@sdas.org
	 *@Version : V1.0
	 *@ModifiedBy 李灿
	 *@Copyright:山东亿云信息技术有限公司--天枢大数据团队
	 *@date 2017年3月20日 下午5:15:23
	 */
	private ModifiableSolrParams getSolrParamsByMySub(String operatorId){
		//获取当前用户的所有订阅列表
		Map<String, Object> paraMap = new HashMap<String, Object>();
		paraMap.put("OPERATOR_ID", operatorId);
		List<Map<String, Object>> mySubList = subService.queryMySubList(paraMap);
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrQuery filterQuery = new SolrQuery();
		int fromDoc = 0;
		int rows = 50;
		filterQuery.setQuery("*:*");
		//根据订阅的内容组装查询条件组装查询
		StringBuffer sb = new StringBuffer();
		for (Map<String, Object> map : mySubList) {
			sb.append("(");
			if (map.containsKey("WEBSITE_NAME") && map.get("WEBSITE_NAME").toString().length()>0) {
				sb.append("websitename:\""+map.get("WEBSITE_NAME").toString()+"\"");
				if (map.containsKey("COLUMN_NAME") && map.get("COLUMN_NAME").toString().length()>0) {
					sb.append(" AND column_name:\""+map.get("COLUMN_NAME").toString()+"\"");
				}
			}
			sb.append(") OR ");
		}
		String fileterQuery = sb.toString();
		if (fileterQuery.length()>3) {
			fileterQuery = fileterQuery.substring(0, fileterQuery.length()-3);
		}
		if (fileterQuery.length()>0) {
			filterQuery.addFilterQuery(fileterQuery);
		}
		filterQuery.addFilterQuery("title:*");//查询标题不为空
		filterQuery.addFilterQuery("content:*");
		filterQuery.addFilterQuery(getLastModified());
		filterQuery.setStart(fromDoc);
		filterQuery.addSort("lastModified", ORDER.desc);
		filterQuery.setRows(rows);
		params.add(filterQuery);
		return params;
	}
	
	/**
	 * 根据关键词查询组装查询条件
	 *@author:李灿--lic@sdas.org
	 *@Version : V1.0
	 *@ModifiedBy 李灿
	 *@Copyright:山东亿云信息技术有限公司--天枢大数据团队
	 *@date 2017年3月1日 下午5:20:22
	 */
	public ModifiableSolrParams getSolrParamsByKeyword(Map<String, Object> paraMap){
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrQuery filterQuery = new SolrQuery();
		int fromDoc = 0;
		int rows = 20;
		if (paraMap.containsKey("keyword") && !StringUtils.isEmpty(paraMap.get("keyword"))) {
			
			/*使用分词进行处理
			 * Result result= ToAnalysis.parse(paraMap.get("keyword").toString());
			String str = "";
			StringBuffer sb = new StringBuffer();
			sb.append("(title:\""+paraMap.get("keyword").toString()+"\" || appc:\""+paraMap.get("keyword").toString()+"\" || fjcontent:\""+paraMap.get("keyword").toString()+"\") || (" );
			for (Term term : result) {
				if (term.getName().trim().length()>0) {
					sb.append("(title:\""+term.getName()+"\" || appc:\""+term.getName()+"\" || fjcontent:\""+term.getName()+"\") && " );
				}
			}
			if(sb.indexOf("&&")>0){
				str = sb.substring(0, sb.length()-4);
				str += ")";
				filterQuery.setQuery(str);
			}else{
				str = sb.substring(0, sb.length()-4);
				filterQuery.setQuery(str);
			}*/
			/*for (String str : s) {
				filterQuery.setQuery("title:")
			}*/
			filterQuery.setQuery("title:\""+paraMap.get("keyword").toString()+"\" || appc:\""+paraMap.get("keyword").toString()+"\"");
		}
		if (paraMap.containsKey("cuPage") && Integer.parseInt(paraMap.get("cuPage").toString()) > 1) {
			fromDoc = rows * (Integer.parseInt(paraMap.get("cuPage").toString())-1);
		}
		filterQuery.addFilterQuery("title:*");//查询标题不为空
		filterQuery.addFilterQuery("content:*");
		filterQuery.setStart(fromDoc);
		filterQuery.addSort("lastModified", ORDER.desc);
		filterQuery.setRows(rows);
		params.add(filterQuery);
		return params;
	}
	
	/**
	 * 根据tagname查询条件组装
	 *@author:李灿--lic@sdas.org
	 *@Version : V1.0
	 *@ModifiedBy 李灿
	 *@Copyright:山东亿云信息技术有限公司--天枢大数据团队
	 *@date 2017年3月3日 上午9:45:49
	 */
	/*public ModifiableSolrParams getSolrParamsByTagName(Map<String, Object> paraMap){
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrQuery filterQuery = new SolrQuery();
		int fromDoc = 0;
		int rows = 10;
		if (paraMap.containsKey("tagName") && !StringUtils.isEmpty(paraMap.get("tagName"))) {
			filterQuery.setQuery("manuallabel:\""+paraMap.get("tagName").toString()+"\"");
		}
		if (paraMap.containsKey("cuPage") && Integer.parseInt(paraMap.get("cuPage").toString()) > 1) {
			fromDoc = rows * (Integer.parseInt(paraMap.get("cuPage").toString())-1);
		}
		filterQuery.addFilterQuery("title:*");//查询标题不为空
		filterQuery.setStart(fromDoc);
		filterQuery.addSort("lastModified", ORDER.desc);
		filterQuery.setRows(rows);
		params.add(filterQuery);
		return params;
	}*/
	

	/**
	 * 获取浏览数，评论数，点赞数
	 */
	@SuppressWarnings({ "rawtypes","unchecked"})
	public List<ContentForSolrj> hitsAndLikeslist(List<ContentForSolrj> list) {
		String ids = "";
		List<Map> newList;
		Map<String, Object> mapIds = new HashMap<String, Object>();
		for(ContentForSolrj content : list){
			ids += "'" + content.getId() + "',";
		}
		if(ids.length() > 0){
			ids = ids.substring(0, ids.length() - 1);
		}
		mapIds.put("ids", ids);
		newList = solrArticleService.queryInIds(mapIds);
		for(ContentForSolrj c : list){
		    for(Map<String, Object> map: newList){
				if((map.get("ARTICLE_ID").toString()).equals(c.getId())){
					c.setHits(Integer.parseInt(map.get("HITS") == null ? "0" : map.get("HITS").toString()));
					c.setLikes(Integer.parseInt(map.get("LIKES") == null ? "0" : map.get("LIKES").toString()));
					c.setComments(Integer.parseInt(map.get("COMMENTS") == null ? "0" : map.get("COMMENTS").toString()));
//					contentList.add(c);
				} 
			}
		}
		return list;
	}
	
	/**
	 * 根据ID进行条件组装
	 *@author:李灿--lic@sdas.org
	 *@Version : V1.0
	 *@ModifiedBy 李灿
	 *@Copyright:山东亿云信息技术有限公司--天枢大数据团队
	 *@date 2017年2月22日 下午4:24:08
	 */
	private ModifiableSolrParams getSolrParamsById(Map<String, Object> paraMap){
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrQuery filterQuery = new SolrQuery();
		int fromDoc = 0;
		int rows = 1;
		if (paraMap.containsKey("id")) {
			filterQuery.setQuery("id:\""+CyptoUtils.decode(paraMap.get("id").toString())+"\"");
		}
		filterQuery.setStart(fromDoc);
		filterQuery.setRows(rows);
		params.add(filterQuery);
		return params;
	}
	
	/**
	 * 
	 */
/*	public Map<String, Object> queryContentListByTagName(Map<String, Object> paraMap){
		Map<String, Object> resultMap = new HashMap<String, Object>();
		long totalDocs = 0;   //返回条数
		//返回的检索的内容接口
		List<ContentForSolrj> contentList = new ArrayList<ContentForSolrj>();;
		SolrClient solrClient = null;
		try {
			solrClient = new HttpSolrClient(SOLR_URL + SOLR_CORE);
			//组装solr的检索条件
			ModifiableSolrParams params = this.getSolrParamsByTagName(paraMap);
			QueryResponse rsp = solrClient.query(params,METHOD.POST);
			if (rsp != null && rsp.getResults() != null && rsp.getResults().size() > 0) {
				totalDocs = rsp.getResults().getNumFound();			
				contentList = rsp.getBeans(ContentForSolrj.class);
			}
			
			contentList = hitsAndLikeslist(contentList);
			//对ID进行加密处理，防止出现在id中存在的?或者&等特殊字符
			for (ContentForSolrj content : contentList) {
				content.setId(CyptoUtils.encode(content.getId()));
				//判断正文内容的长度是不是大于100，如果大于100，怎截取前100个字符传至前台，用于节省流量
				content.setContent(content.getContent().length()>100?content.getContent().substring(0, 100):content.getContent());
			}
			resultMap.put("contentForSolr", contentList);
			resultMap.put("totalDocs", totalDocs);
			resultMap.put("tagName", paraMap.get("tagName"));
		} catch (Exception e) {
			System.out.println("检索出现异常！");
			e.printStackTrace();
		} finally{
			if (solrClient != null) {
				try {
					solrClient.close();
				} catch (Exception e2) {
					System.out.println("Solr客户端关闭失败！");
					e2.printStackTrace();
				}
			}
		}
		return resultMap;
	}*/
	
	/**
	 * 获取全文检索字段lastModified的时间范围
	 * @author 李庆忠
	 * @date 2016年5月26日 下午2:19:03
	 * @return 时间范围（格式为[yyyy-MM-ddTHH:mm:ssZ TO yyyy-MM-ddTHH:mm:ssZ]）
	 */
	/**
	 * 获取全文检索字段lastModified的时间范围
	 * @author 李庆忠
	 * @date 2016年5月26日 下午2:19:03
	 * @return 时间范围（格式为[yyyy-MM-ddTHH:mm:ssZ TO yyyy-MM-ddTHH:mm:ssZ]）
	 */
	private static String getLastModified() {
	
		String lastModified = null;
		SimpleDateFormat ymdhms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat ymd = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat hms = new SimpleDateFormat("HH:mm:ss");
		Calendar c = Calendar.getInstance();
		try {
			c.setTime(ymdhms.parse(ymd.format(c.getTime()) + " 23:59:59"));
			c.add(Calendar.DATE, -1); // 昨天
			Date date = c.getTime();
			 SimpleDateFormat tmp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT);
		      tmp.setTimeZone(UTC);
			String endDate = tmp.format(date);
			
			c.setTime(ymdhms.parse(ymd.format(c.getTime()) + " 00:00:00"));			
			date = c.getTime();
			String startDate = tmp.format(date);
			lastModified = "lastModified:" + "[" + startDate + " TO "+ endDate + "]";
		} catch (Exception e) {
			System.out.println("时间处理出错！");
			e.printStackTrace();
		}
		return lastModified;
	}

	/**
	 * 获取查询区间
	 * @author 李庆忠
	 * @param interval 时间区间
	 * @return 查询区间
	 */
	/*private String getLastModified(String interval) {		
		int i = Integer.parseInt(interval);
		String lastModified = null;
		SimpleDateFormat ymdhms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat ymd = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat hms = new SimpleDateFormat("HH:mm:ss");
		Calendar c = Calendar.getInstance();
		try {
			c.setTime(ymdhms.parse(ymd.format(c.getTime()) + " 23:59:59"));
			c.add(Calendar.DATE, - 1);
			Date date = c.getTime();
			String endDate = ymd.format(date) + "T" + hms.format(date) + "Z";
			
			c.setTime(ymdhms.parse(ymd.format(c.getTime()) + " 00:00:00"));
			c.add(Calendar.DATE, - (i - 1));
			date = c.getTime();
			String startDate = ymd.format(date) + "T" + hms.format(date) + "Z";
			lastModified = "lastModified:" + "[" + startDate + " TO "+ endDate + "]";
		} catch (Exception e) {
			System.out.println("设置查询区间失败!");
			e.printStackTrace();
		}
		return lastModified;
	}*/

	
}
