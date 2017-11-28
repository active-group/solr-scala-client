package com.github.takezoe.solr.scala

import org.apache.solr.client.solrj.{SolrClient => ApacheSolrClient}
import org.apache.solr.common.SolrInputDocument
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class BatchRegisterSuite extends FunSuite with MockitoSugar {

  val collectionName = Some("collectionName")

  test("commit calls SolrServer#commit."){
    val solr = mock[ApacheSolrClient]
    val register = new BatchRegister(solr, collectionName)
    register.commit()

    val captor = ArgumentCaptor.forClass(classOf[String])
    verify(solr, times(1)).commit(captor.capture())

    assert(captor.getValue == collectionName.get)
  }

  test("rollback calls SolrServer#commit."){
    val solr = mock[ApacheSolrClient]
    val register = new BatchRegister(solr, collectionName)
    register.rollback()

    val captor = ArgumentCaptor.forClass(classOf[String])
    verify(solr, times(1)).rollback(captor.capture())
    assert(captor.getValue == collectionName.get)
  }

  test("add a document via the constructor."){
    val solr = mock[ApacheSolrClient]
    val register = new BatchRegister(solr, collectionName, Map("id" -> "123"))

    val captor = ArgumentCaptor.forClass(classOf[SolrInputDocument])
    val collectionNameCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(solr, times(1)).add(collectionNameCaptor.capture(), captor.capture())
    assert(collectionNameCaptor.getValue == collectionName.get)
    val doc = captor.getValue
    assert(doc.getField("id").getValue() == "123")
  }

  test("add documents via the constructor."){
    val solr = mock[ApacheSolrClient]
    val register = new BatchRegister(solr, collectionName, Map("id" -> "123"), Map("id" -> "456"))

    val captor = ArgumentCaptor.forClass(classOf[SolrInputDocument])
    val collectionNameCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(solr, times(2)).add(collectionNameCaptor.capture(), captor.capture())
    assert(collectionNameCaptor.getValue == collectionName.get)

    val doc1 = captor.getAllValues().get(0)
    assert(doc1.getField("id").getValue() == "123")

    val doc2 = captor.getAllValues().get(1)
    assert(doc2.getField("id").getValue() == "456")
  }
}