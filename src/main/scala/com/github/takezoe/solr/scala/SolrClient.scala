package com.github.takezoe.solr.scala

import com.github.takezoe.solr.scala.query.{DefaultExpressionParser, ExpressionParser, QueryTemplate}
import org.apache.solr.client.solrj.{SolrClient => ApacheSolrClient}
import org.apache.solr.client.solrj.impl.{HttpSolrClient => ApacheHttpSolrClient}

/**
 * This is the simple Apache Solr client for Scala.
 */
class SolrClient(url: String)
  (implicit factory: (String) => ApacheSolrClient = { (url: String) => new ApacheHttpSolrClient(url) },
            parser: ExpressionParser = new DefaultExpressionParser()) {

  private val server = factory(url)
  //initializer(server)

  /**
   * Shutdown this solr client to release allocated resources.
   */
  def shutdown(): Unit = server.close()

  /**
   * Execute given operation in the transaction.
   * 
   * The transaction is committed if operation was successful. 
   * But the transaction is rolled back if an error occurred.
   */
  def withTransaction[T](operations: => T): T = {
    try {
      val result = operations
      commit()
      result
    } catch {
      case t: Throwable => {
        rollback()
        throw t
      }
    }
  }
  
  /**
   * Search documents using the given query.
   *
   * {{{
   * import jp.sf.amateras.solr.scala._
   *
   * val client = new SolrClient("http://localhost:8983/solr")
   *
   * val result: List[Map[String, Any]] =
   *   client.query("*:*")
   *         .fields("id", "manu", "name")
   *         .sortBy("id", Order.asc)
   *         .getResultAsMap()
   * }}}
   */
  def query(query: String): QueryBuilder = new QueryBuilder(server, None, query)

  def query(collectionName: String, query: String): QueryBuilder = new QueryBuilder(server, Some(collectionName), query)

  /**
   * Execute batch updating.
   *
   * Note: To register documents actual, you have to call commit after added them.
   *
   * {{{
   * import jp.sf.amateras.solr.scala._
   *
   * val client = new SolrClient("http://localhost:8983/solr")
   *
   * client.add(Map("id"->"001", "manu" -> "Lenovo", "name" -> "ThinkPad X201s"))
   *       .add(Map("id"->"002", "manu" -> "Lenovo", "name" -> "ThinkPad X202"))
   *       .add(Map("id"->"003", "manu" -> "Lenovo", "name" -> "ThinkPad X100e"))
   *       .commit
   * }}}
   */
  def add(docs: Any*): BatchRegister = new BatchRegister(server, None, CaseClassMapper.toMapArray(docs: _*): _*)

  def add(collectionName: String, docs: Any*): BatchRegister = new BatchRegister(server, Some(collectionName), CaseClassMapper.toMapArray(docs: _*): _*)

  /**
   * Add documents and commit them immediately.
   *
   * @param docs documents to register
   */
  def register(docs: Any*): Unit = new BatchRegister(server, None, CaseClassMapper.toMapArray(docs: _*): _*).commit()

  def register(collectionName: String, docs: Any*): Unit = new BatchRegister(server, Some(collectionName), CaseClassMapper.toMapArray(docs: _*): _*).commit()

  /**
   * Delete the document which has a given id.
   *
   * @param id the identifier of the document to delete
   */
  def deleteById(id: String): Unit = server.deleteById(id)

  /**
   * Delete documents by the given query.
   *
   * @param query the solr query to select documents which would be deleted
   * @param params the parameter map which would be given to the query
   */
  def deleteByQuery(query: String, params: Map[String, Any] = Map()): Unit = {
    server.deleteByQuery(new QueryTemplate(query).merge(params))
  }

  /**
   * Commit the current session.
   */
  def commit(): Unit = server.commit

  def commit(collectionName: String): Unit = server.commit(collectionName)

  /**
   * Rolled back the current session.
   */
  def rollback(): Unit = server.rollback

  def rollback(collectionName: String): Unit = server.rollback(collectionName)
}
