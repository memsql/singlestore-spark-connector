package com.memsql.spark.pushdown

import com.memsql.spark.connector.dataframe.TypeConversions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, expressions}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

/**
 * Convenience methods for creating a SQLBuilder
 */
object SQLBuilder {
  /**
   * Create a SQLBuilder from a SQL expression string along with any associated params.
   * @param sql The SQL expression to initialize the SQLBuilder with
   * @param params Any params for the provided expression
   */
  def fromStatic(sql: String, params: Seq[Any]=Nil): SQLBuilder =
    new SQLBuilder().raw(sql).addParams(params)

  def withAlias(alias: QueryAlias, inner: SQLBuilder => Unit): SQLBuilder =
    new SQLBuilder().aliasedBlock(alias, inner)

  def withFields(fields: Seq[NamedExpression]): SQLBuilder =
    new SQLBuilder(fields)

  /**
   * We need this destructor since the BinaryOperator object is private to Spark
   * TODO: Remove when Spark makes it public (maybe 1.6)
   */
  object BinaryOperator {
    def unapply(e: expressions.BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
  }
}

/**
 * SQLBuilder is a mutable object for efficiently building up complex SQL expressions.
 * Internally it uses StringBuilder and a ListBuffer to efficiently support many small writes.
 * All methods return `this` for chaining.
 */
class SQLBuilder(fields: Seq[NamedExpression]=Nil) {
  var sql: StringBuilder = new StringBuilder
  var params: ListBuffer[Any] = ListBuffer.empty[Any]

  /**
    * Appends another SQLBuilder to this one.
    * @param other The other SQLBuilder
    * @return This SQLBuilder
    */
  def appendBuilder(other: SQLBuilder): SQLBuilder =
    raw(other.sql).addParams(other.params)

  /**
   * Appends another SQLBuilder to this one.
   * @param other The other SQLBuilder
   * @param ifNone A string to append to this SQLBuilder if other is None
   * @return This SQLBuilder
   */
  def maybeAppendBuilder(other: Option[SQLBuilder], ifNone: String=""): SQLBuilder = other match {
    case Some(o) => raw(" ").raw(o.sql).addParams(o.params)
    case None => raw(ifNone)
  }

  /**
   * Adds a named expression to the builder.
   * @note Attempts to lookup the expression in [[fields]] if it exists.
   * @note The expression is fully qualified if possible (ex. `foo`.`bar`)
   */
  def attr(a: Attribute): SQLBuilder = {
    fields.find(e => e.exprId == a.exprId) match {
      case Some(resolved) =>
        qualifiedAttr(resolved.qualifiers.headOption, resolved.name)
      case None =>
        qualifiedAttr(a.qualifiers.headOption, a.name)
    }
    this
  }

  /**
    * A String attribute optionally qualified by another string.
    */
  def qualifiedAttr(qualifier: Option[String], name: String): SQLBuilder = {
    qualifier.map(q => identifier(q).raw("."))
    identifier(name)
  }

  /**
    * Escapes and appends a single identifier to the SQLBuilder.
    */
  def identifier(a: String): SQLBuilder = { sql.append("`").append(a).append("`"); this }

  /**
   * Adds a raw string to the expression, no escaping occurs.
   */
  def raw(s: String): SQLBuilder = { sql.append(s); this }

  /**
   * @see SQLBuilder#raw(String)
   */
  def raw(s: StringBuilder): SQLBuilder = { sql.append(s); this }

  /**
   * Wraps the provided lambda in parentheses.
   * @param inner A lambda which will be executed between adding parenthesis to the expression
   */
  def block(inner: => Unit): SQLBuilder = { raw("("); inner; raw(")"); this }

  /**
    * Wraps the provided lambda with a non-qualified alias.
    * @param alias The alias to give the inner expression
    * @param inner A lambda which will be wrapped with ( ... ) AS alias
    */
  def aliasedBlock(alias: String, inner: SQLBuilder => Unit): SQLBuilder =
    block({ inner(this) }).raw(" AS ").identifier(alias)

  /**
    * @see SQLBuilder#aliasedBlock(String, SQLBuilder => Unit)
    */
  def aliasedBlock(alias: QueryAlias, inner: SQLBuilder => Unit): SQLBuilder =
    aliasedBlock(alias.toString, inner)

  /**
   * Adds the provided param to the internal params list.
   */
  def param(p: Any): SQLBuilder = { params += p; this }

  /**
   * @see SQLBuilder#param(Any)
   * @note Use this variant if the parameter is one of the Catalyst Types
   */
  def param(p: Any, t: DataType): SQLBuilder = {
    params += CatalystTypeConverters.convertToScala(p, t)
    this
  }

  /**
   * @see SQLBuilder#param(Any)
   * @note Handles converting the literal from its Catalyst Type to a Scala Type
   */
  def param(l: Literal): SQLBuilder = {
    params += CatalystTypeConverters.convertToScala(l.value, l.dataType)
    this
  }

  /**
   * Add a list of params directly to the internal params list.
   * Optimized form of calling SQLBuilder#param(Any) for each element of newParams.
   */
  def addParams(newParams: Seq[Any]): SQLBuilder = { params ++= newParams; this }

  /**
   * Adds a list of elements to the SQL expression, as a comma-delimited list.
   * Handles updating the expression as well as adding
   * each param to the internal params list.
   *
   * @param newParams A list of objects to add to the expression
   * @param t The Catalyst type for all of the objects
   */
  def paramList(newParams: Seq[Any], t: DataType): SQLBuilder = {
    block {
      for (j <- newParams.indices) {
        if (j != 0) {
          raw(", ?").param(newParams(j), t)
        }
        else {
          raw("?").param(newParams(j), t)
        }
      }
    }
  }

  /**
   * Adds a list of expressions to this SQLBuilder, joins each expression with the provided conjunction.
   * @param expressions A list of [[Expression]]s
   * @param conjunction A string to join the resulting SQL Expressions with
   */
  def addExpressions(expressions: Seq[Expression], conjunction: String): SQLBuilder = {
    for (i <- expressions.indices) {
      if (i != 0) {
        raw(conjunction)
      }
      addExpression(expressions(i))
    }
    this
  }

  def maybeAddExpressions(expressions: Seq[Expression], conjunction: String): Option[SQLBuilder] = {
    if (expressions.nonEmpty) {
      Some(addExpressions(expressions, conjunction))
    } else {
      None
    }
  }

  /**
   * Adds a Catalyst [[Expression]] to this SQLBuilder
   * @param expression The [[Expression]] to add
   */
  def addExpression(expression: Expression): SQLBuilder = {
    expression match {
      case a: Attribute => attr(a)
      case l: Literal => raw("?").param(l)
      case Alias(child: Expression, name: String) =>
        block { addExpression(child) }.raw(" AS ").identifier(name)

      case Cast(child, t) => t match {
        case TimestampType | DateType => block {
          raw("UNIX_TIMESTAMP")
          block { addExpression(child) }
          // JDBC Timestamp is in nanosecond precision, but MemSQL is microsecond
          raw(" * 1000")
        }
        case _ => TypeConversions.DataFrameTypeToMemSQLCastType(t) match {
          case None => addExpression(child)
          case Some(memsqlType) => {
            raw("CAST").block {
              addExpression(child)
                .raw(" AS ")
                .raw(memsqlType)
            }
          }
        }
      }

      case Not(inner) => raw("NOT").block { addExpressions(Seq(inner), " AND ") }

      case StartsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        attr(a).raw(" LIKE ?").param(s"${v.toString}%")
      case EndsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        attr(a).raw(" LIKE ?").param(s"%${v.toString}")
      case Contains(a: Attribute, Literal(v: UTF8String, StringType)) =>
        attr(a).raw(" LIKE ?").param(s"%${v.toString}%")

      case IsNull(a: Attribute) => attr(a).raw(" IS NULL")
      case IsNotNull(a: Attribute) => attr(a).raw(" IS NOT NULL")

      case InSet(a: Attribute, set) => attr(a).raw(" IN ").paramList(set.toSeq, a.dataType)

      case In(a: Attribute, list) if list.forall(_.isInstanceOf[Literal]) =>
        attr(a).raw(" IN ").paramList(list, a.dataType)

      // AGGREGATE PATTERNS

      case Count(child) => raw("COUNT").block { addExpression(child) }
      case CountDistinct(children) => raw("COUNT(DISTINCT ").addExpressions(children, ", ").raw(")")

      // NOTE: MemSQL does not allow the user to configure relativeSD,
      // so we only can pushdown if the user asks for up to the maximum
      // precision that we offer.
      case ApproxCountDistinct(child, relativeSD) if relativeSD >= 0.01 =>
        raw("APPROX_COUNT_DISTINCT").block { addExpression(child) }

      case Sum(child) => raw("SUM").block { addExpression(child) }
      case Average(child) => raw("AVG").block { addExpression(child) }
      case Max(child) => raw("MAX").block { addExpression(child) }
      case Min(child) => raw("MIN").block { addExpression(child) }

      case SortOrder(child: Expression, Ascending) => block { addExpression(child) }.raw(" ASC")
      case SortOrder(child: Expression, Descending) => block { addExpression(child) }.raw(" DESC")

      // BINARY OPERATORS

      case MaxOf(left, right) => raw("GREATEST").block { addExpressions(Seq(left, right), ", ") }
      case MinOf(left, right) => raw("LEAST").block { addExpressions(Seq(left, right), ", ") }

      // a pmod b := (a % b + b) % b
      case Pmod(left, right) =>
        block {
          block {
            addExpressions(Seq(left, right), " % ")
            raw(" + ")
            addExpression(right)
          }.raw(" % ").addExpression(right)
        }

      // All Spark BinaryOperator symbols work in SQL expressions, except for the three handled above
      case op @ SQLBuilder.BinaryOperator(left: Expression, right: Expression) =>
        block { addExpression(left).raw(s" ${op.symbol} ").addExpression(right) }
    }
    this
  }
}
