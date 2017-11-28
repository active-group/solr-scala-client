package com.github.takezoe.solr.scala

import org.apache.solr.client.solrj.{SolrClient => ApacheSolrClient}
import org.apache.solr.common.SolrInputDocument

class BatchRegister(server: ApacheSolrClient, collectionName: Option[String], docs: Map[String, Any]*) {

  add(docs: _*)

  def add(docs: Any*): BatchRegister = {
    CaseClassMapper.toMapArray(docs: _*).foreach { doc =>
      val solrDoc = new SolrInputDocument
      doc.foreach { case (key, value) =>
        solrDoc.addField(key, value)
      }
      performAction(() => server.add(solrDoc), cn => server.add(cn, solrDoc))
    }
    this
  }

  def commit(): Unit = performAction(() => server.commit(), cn => server.commit(cn))

  def rollback(): Unit = performAction(() => server.rollback(), cn => server.rollback(cn))

  private def performAction(withoutCollection: () => Unit, withCollection: String => Unit): Unit = {
    collectionName match {
      case Some(name) => withCollection(name)
      case None => withoutCollection()
    }
  }
}