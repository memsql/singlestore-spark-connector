package com.memsql.spark.pushdown

import com.memsql.spark.connector.MemSQLCluster
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import StringBuilderImplicits._
import org.apache.spark.sql.memsql.{MemSQLContext, MemSQLRelation}

abstract class AbstractQuery {
  def alias: QueryAlias
  def output: Seq[Attribute]
  def collapse: SQLBuilder

  def qualifiedOutput: Seq[Attribute] = output.map(
    a => AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, Seq(alias.toString)))

  /**
   * Performs a pre-order traversal of the query tree
   * and returns the first non-None result from the query
   * function.
   */
  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T]

  def prettyPrint(depth: Int, builder: StringBuilder): Unit

  def sharesCluster(otherTree: AbstractQuery): Boolean = {
    val result = for {
      myBase <- find { case q: BaseQuery => q }
      otherBase <- otherTree.find { case q: BaseQuery => q }
    } yield {
      myBase.cluster.getMasterInfo == otherBase.cluster.getMasterInfo
    }

    result.getOrElse(false)
  }
}

case class BaseQuery(alias: QueryAlias, relation: MemSQLRelation, output: Seq[Attribute]) extends AbstractQuery {
  val cluster: MemSQLCluster = relation.cluster
  val database: Option[String] = relation.database

  val query: SQLBuilder = SQLBuilder.fromStatic(relation.query, Nil)

  override def collapse: SQLBuilder =
    SQLBuilder.withAlias(alias, b => b.appendBuilder(query))

  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T] = query.lift(this)

  override def prettyPrint(depth: Int, builder: StringBuilder): Unit =
    builder
      .indent(depth)
      .append(s"BaseQuery[$alias, ${output.mkString(",")}] (")
      .append(query.sql)
      .append(")\n")
}

case class PartialQuery(alias: QueryAlias,
                        output: Seq[Attribute],
                        prefix: Option[SQLBuilder] = None,
                        suffix: Option[SQLBuilder] = None,
                        inner: AbstractQuery) extends AbstractQuery {

  override def collapse: SQLBuilder = {
    SQLBuilder.withAlias(alias, b => {
      b.raw("SELECT ")
        .maybeAppendBuilder(prefix, "*")
        .raw(" FROM ")
        .appendBuilder(inner.collapse)
        .maybeAppendBuilder(suffix)
    })
  }

  override def prettyPrint(depth: Int, builder: StringBuilder): Unit = {
    builder
      .indent(depth)
      .append(s"PartialQuery[$alias, ${output.mkString(",")}] (")
      .append(prefix.map(_.sql).getOrElse(""))
      .append(") (")
      .append(suffix.map(_.sql).getOrElse(""))
      .append(")\n")
    inner.prettyPrint(depth + 1, builder)
  }

  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T] =
    query.lift(this).orElse(inner.find(query))
}

case class JoinQuery(alias: QueryAlias,
                     output: Seq[Attribute],
                     condition: Option[SQLBuilder],
                     left: AbstractQuery,
                     right: AbstractQuery) extends AbstractQuery {

  override def collapse: SQLBuilder =
    SQLBuilder.withAlias(alias, b => {
      b.raw(s"SELECT ${left.alias}.*, ${right.alias}.* FROM ")
        .appendBuilder(left.collapse)
        .raw(" INNER JOIN ")
        .appendBuilder(right.collapse)
        .raw(" WHERE ")
        .maybeAppendBuilder(condition, "1")
    })

  override def prettyPrint(depth: Int, builder: StringBuilder): Unit = {
    builder
      .indent(depth)
      .append(s"JoinQuery[$alias, ${output.mkString(",")}] (")
      .append(condition.map(_.sql).getOrElse(""))
      .append(")\n")
    left.prettyPrint(depth + 1, builder)
    right.prettyPrint(depth + 1, builder)
  }

  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T] =
    query.lift(this)
      .orElse(left.find(query))
      .orElse(right.find(query))
}
