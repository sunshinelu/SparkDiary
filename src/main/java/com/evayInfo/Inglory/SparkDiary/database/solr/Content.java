package com.evayInfo.Inglory.SparkDiary.database.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.Date;

/**
 * Created by sunlu on 17/6/21.
 */
public class Content {
    @Field
    private String id;    //id

    @Field
    private Date lastModified;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }





}
