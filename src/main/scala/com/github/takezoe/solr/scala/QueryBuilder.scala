package com.github.takezoe.solr.scala

import com.github.takezoe.solr.scala.query.{ExpressionParser, QueryTemplate}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.{SolrClient => ApacheSolrClient}

class QueryBuilder(server: ApacheSolrClient, collectionName: Option[String], query: String)(implicit parser: ExpressionParser)
  extends QueryBuilderBase[QueryBuilder] {

  protected def createCopy = new QueryBuilder(server, collectionName, query)(parser)

  /**
   * Returns the search result of this query as List[Map[String, Any]].
   *
   * @param params the parameter map or case class which would be given to the query
   * @return the search result
   */
  def getResultAsMap(params: Any = null): MapQueryResult = {
    solrQuery.setQuery(new QueryTemplate(query).merge(CaseClassMapper.toMap(params)))
    responseToMap(performAction(() => server.query(solrQuery), cn => server.query(cn, solrQuery)))
  }

  /**
   * Returns the search result of this query as the case class.
   *
   * @param params the parameter map or case class which would be given to the query
   * @return the search result
   */
  def getResultAs[T](params: Any = null)(implicit m: Manifest[T]): CaseClassQueryResult[T] = {
    solrQuery.setQuery(new QueryTemplate(query).merge(CaseClassMapper.toMap(params)))
    responseToObject(performAction(() => server.query(solrQuery), cn => server.query(cn, solrQuery)))
  }

  private def performAction(withoutCollection: () => QueryResponse, withCollection: String => QueryResponse): QueryResponse = {
    collectionName match {
      case Some(name) => withCollection(name)
      case None => withoutCollection()
    }
  }
}

