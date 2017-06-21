package com.evayInfo.Inglory.SparkDiary.database.solr;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.solr.client.solrj.beans.Field;

/**
 * TODO:通过SolrJ获取过来的内容对应的bean
 * 作者：李灿    lic@sdas.org
 * 时间：2016年5月18日 上午9:07:02
 * 修改人：李灿    lic@sdas.org
 * 修改时间：2017年2月16日09:46:21
 * 修改内容：注释掉了source、msgtype两个不知道作用的字段和一个暂时过时不用的字段label
 *        同时添加了websitelb、websitejb、manuallabel、xzqhname，
 *        marks的值由原来是有label字段中获取，改成由manuallabel中获取
 */
public class ContentForSolrj {

	@Field
	private String id;    //id
	@Field
	private List<String> title;   //标题
	@Field
	private String content;    //正文
	/*@Field
	private String fjcontent;*/
	@Field
	private String url;     //原URL
	/*@Field
	private String source;   
	@Field
	private String msgtype;*/
	@Field
	private Date lastModified;  //修改时间
	@Field
	private String websitelb;   //网站类别
	@Field
	private String websitejb;   //网站级别（国家级、省级、市级）
	@Field
	private String manuallabel;   //内容标签（科技、人才、战略资讯、招投标）
	@Field
	private String xzqhname;   //网站所属区域
	@Field
	private String websitename;//网站名称
	
	/*@Field
	private String label; */    //关键词
	
	private List<String> marks;  //每个关键词
	
	private int hits = 0;     //点击次数
	
	private int likes = 0;    //点赞次数
	
	private int comments = 0; //评论数
	
	private String imgUrl;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public List<String> getTitle() {
		return title;
	}
	public void setTitle(List<String> title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	/*public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getMsgtype() {
		return msgtype;
	}
	public void setMsgtype(String msgtype) {
		this.msgtype = msgtype;
	}*/
	public Date getLastModified() {
		return lastModified;
	}
	public void setLastModified(Date lastModified) {
		this.lastModified = lastModified;
	}
	public String getWebsitelb() {
		return websitelb;
	}
	public void setWebsitelb(String websitelb) {
		this.websitelb = websitelb;
	}
	public String getWebsitejb() {
		return websitejb;
	}
	public void setWebsitejb(String websitejb) {
		this.websitejb = websitejb;
	}
	public String getManuallabel() {
		return manuallabel;
	}
	public void setManuallabel(String manuallabel) {
		this.manuallabel = manuallabel;
	}
	public String getXzqhname() {
		return xzqhname;
	}
	public void setXzqhname(String xzqhname) {
		this.xzqhname = xzqhname;
	}
	/*public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}*/
	public List<String> getMarks() {
		return marks;
	}
	public void setMarks(List<String> marks) {
		this.marks = marks;
	}
	
	public void setMarks(String manuallabel){
		String[] mark = manuallabel.split(";");
		List<String> marksList = new ArrayList<String>();
		for (String string : mark) {
			marksList.add(string);
		}
		this.marks = marksList;
	}
	public String getWebsitename() {
		return websitename;
	}
	public void setWebsitename(String websitename) {
		this.websitename = websitename;
	}
	public int getHits() {
		return hits;
	}
	public void setHits(int hits) {
		this.hits = hits;
	}
	public int getLikes() {
		return likes;
	}
	public void setLikes(int likes) {
		this.likes = likes;
	}
	public int getComments() {
		return comments;
	}
	public void setComments(int comments) {
		this.comments = comments;
	}
	public String getImgUrl() {
		return imgUrl;
	}
	public void setImgUrl(String imgUrl) {
		this.imgUrl = imgUrl;
	}
	
}
