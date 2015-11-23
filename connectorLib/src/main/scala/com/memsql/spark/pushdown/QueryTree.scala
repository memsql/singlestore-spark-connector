package com.memsql.spark.pushdown

import com.memsql.spark.connector.util.MemSQLConnectionInfo
import com.memsql.spark.context.{MemSQLContext, MemSQLNode}
import org.apache.spark.sql.MemSQLRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import StringBuilderImplicits._

abstract class AbstractQuery {
  def output: Seq[Attribute]
  def collapse(alias: QueryAlias): SQLBuilder

  /**
   * Performs a pre-order traversal of the query tree
   * and returns the first non-None result from the query
   * function.
   *
   * @note Since query is a PartialFunction, you don't need
   * explicitly return None if you just want to match on a
   * node in the tree. See [[getConnectionInfo]] for an example.
   */
  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T]

  def prettyPrint(depth: Int, builder: StringBuilder): Unit

  def getConnectionInfo: Option[MemSQLConnectionInfo] =
    find { case q: BaseQuery => q.connectionInfo }

  def sharesCluster(otherTree: AbstractQuery): Boolean = {
    val result = for {
      (myMasterAgg, myCxnInfo) <- find {
        case q: BaseQuery => (q.getMasterAgg, q.connectionInfo)
      }
      (otherMasterAgg, otherCxnInfo) <- otherTree.find {
        case q: BaseQuery => (q.getMasterAgg, q.connectionInfo)
      }
    } yield {
      (myCxnInfo == otherCxnInfo) ||
      (myMasterAgg.isDefined && otherMasterAgg.isDefined && myMasterAgg == otherMasterAgg)
    }

    result.getOrElse(false)
  }
}

case class BaseQuery(relation: MemSQLRelation) extends AbstractQuery {
  val output: Seq[Attribute] = relation.output

  val query: SQLBuilder = SQLBuilder.fromStatic(relation.query, relation.queryParams)

  val connectionInfo: MemSQLConnectionInfo = relation.connectionInfo

  override def collapse(alias: QueryAlias) = query

  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T] = query.lift(this)

  /**
   * If the underlying SQLContext is a MemSQLContext, return the
   * associated Master Aggregator, else return None
   */
  def getMasterAgg: Option[MemSQLNode] = relation.sqlContext match {
    case m: MemSQLContext => Some(m.masterAgg)
    case _ => None
  }

  override def prettyPrint(depth: Int, builder: StringBuilder) =
    builder
      .indent(depth)
      .append(s"BaseQuery[${output.mkString(",")}] ")
      .append(query.sql)
      .append("\n")
}

case class PartialQuery(output: Seq[Attribute],
                        prefix: Option[SQLBuilder]=None,
                        suffix: Option[SQLBuilder]=None,
                        inner: AbstractQuery) extends AbstractQuery {

  override def collapse(alias: QueryAlias) = {
    if (prefix.isEmpty && suffix.isEmpty) {
      inner.collapse(alias.next)
    } else {
      new SQLBuilder()
        .raw("SELECT ")
        .appendBuilder(prefix.getOrElse(SQLBuilder.fromStatic("*")))
        .raw(" FROM (")
        .appendBuilder(inner.collapse(alias.next))
        .raw(s") $alias ")
        .appendBuilder(suffix.getOrElse(new SQLBuilder))
    }
  }

  override def prettyPrint(depth: Int, builder: StringBuilder) = {
    builder
      .indent(depth)
      .append(s"PartialQuery[${output.mkString(",")}] PREFIX[")
      .append(prefix.map(_.sql).getOrElse(""))
      .append("] SUFFIX[")
      .append(suffix.map(_.sql).getOrElse(""))
      .append("]\n")
    inner.prettyPrint(depth + 1, builder)
  }

  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T] =
    query.lift(this).orElse(inner.find(query))
}

case class JoinQuery(output: Seq[Attribute],
                     condition: Option[SQLBuilder],
                     left: AbstractQuery,
                     right: AbstractQuery) extends AbstractQuery {

  override def collapse(alias: QueryAlias) = {
    val (leftAlias, rightAlias) = alias.fork

    val builder = new SQLBuilder()
      .raw(s"SELECT $leftAlias.*, $rightAlias.* FROM (")
      .appendBuilder(left.collapse(leftAlias.next))
      .raw(s") AS $leftAlias INNER JOIN (")
      .appendBuilder(right.collapse(rightAlias.next))
      .raw(s") AS $rightAlias")

    if (condition.isDefined) {
      builder.raw(" WHERE ").appendBuilder(condition.get)
    }
    builder
  }

  override def prettyPrint(depth: Int, builder: StringBuilder) = {
    builder
      .indent(depth)
      .append(s"JoinQuery[${output.mkString(",")}] CONDITION[")
      .append(condition.map(_.sql).getOrElse(""))
      .append("]\n")
    left.prettyPrint(depth + 1, builder)
    right.prettyPrint(depth + 1, builder)
  }

  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T] =
    query.lift(this)
      .orElse(left.find(query))
      .orElse(right.find(query))
}
