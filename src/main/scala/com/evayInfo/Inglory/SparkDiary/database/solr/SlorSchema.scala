package com.evayInfo.Inglory.SparkDiary.database.solr

import java.util.Date

import org.apache.solr.client.solrj.beans.Field

/**
 * Created by sunlu on 17/9/7.
 */
class SlorSchema {
  @Field private var id: String = null
  @Field private var lastModified: Date = null

  def getId: String = {
    return id
  }

  def setId(id: String) {
    this.id = id
  }

  def getLastModified: Date = {
    return lastModified
  }

  def setLastModified(lastModified: Date) {
    this.lastModified = lastModified
  }
}
