package com.memsql.spark

import java.sql.{Date, SQLSyntaxErrorException, Timestamp}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{breakOut, mutable}

object SQLGen extends LazyLogging {
  type VariableList = List[Var[_]]

  val aliasGen = Iterator.from(1).map(i => s"a$i")

  trait Joinable {
    def +(j: Joinable): Statement
    def +(s: String): Statement = this + Raw(s)
  }

  trait Chunk extends Joinable {
    def +(j: Joinable): Statement = j match {
      case Statement(list, output) => Statement(list ::: this :: Nil, output)
      case c: Chunk                => Statement(c :: this :: Nil)
    }

    def toSQL(fieldMap: Map[ExprId, Attribute]): String
  }

  case class Statement(list: List[Chunk], output: Seq[AttributeReference] = Nil)
      extends Joinable
      with LazyLogging {

    lazy val reverseList: List[Chunk] = list.reverse

    lazy val relations: Seq[Relation] = reverseList.collect {
      case r: Relation => r
    }

    lazy val fieldMap: Map[ExprId, Attribute] = relations
      .flatMap(_.output)
      .map(a => (a.exprId, a))(breakOut)

    lazy val variables: VariableList =
      reverseList.collect {
        case r: Relation => r.reader.variables
        case r: Var[_]   => Iterator(r)
      }.flatten

    lazy val sql: String = reverseList.map(_.toSQL(fieldMap)).mkString(" ")

    def asLogicalPlan(isFinal: Boolean = false): LogicalPlan =
      relations.head.toLogicalPlan(output, sql, variables, isFinal)

    private def newlineIfEmpty: String = list match {
      case Nil => ""
      case _   => "\n"
    }

    // ------------------------------------
    // Builder functions for easy chaining
    // ------------------------------------

    def +(j: Joinable): Statement = j match {
      case Statement(otherList, _) => copy(otherList ::: list)
      case c: Chunk                => copy(c :: list)
    }

    def withLogicalPlanComment(plan: LogicalPlan): Statement =
      this + s"${newlineIfEmpty}-- Spark LogicalPlan: ${plan.simpleString}"

    def selectAll(): Statement                 = this + "\nSELECT *"
    def select(c: Joinable): Statement         = this + "\nSELECT" + c
    def select(c: Option[Joinable]): Statement = this + "\nSELECT" + c.getOrElse(Raw("*"))

    def from(c: Joinable): Statement = this + "\nFROM" + c

    def join(c: Joinable, joinType: JoinType): Statement =
      joinType match {
        case Inner                   => this + "\nINNER JOIN" + c
        case Cross                   => this + "\nCROSS JOIN" + c
        case LeftOuter               => this + "\nLEFT OUTER JOIN" + c
        case RightOuter              => this + "\nRIGHT OUTER JOIN" + c
        case FullOuter               => this + "\nFULL OUTER JOIN" + c
        case NaturalJoin(Inner)      => this + "\nNATURAL JOIN" + c
        case NaturalJoin(LeftOuter)  => this + "\nNATURAL LEFT OUTER JOIN" + c
        case NaturalJoin(RightOuter) => this + "\nNATURAL RIGHT OUTER JOIN" + c
        case NaturalJoin(FullOuter)  => this + "\nNATURAL FULL OUTER JOIN" + c
        case _                       => throw new IllegalArgumentException(s"join type $joinType not supported")
      }

    def on(c: Joinable): Statement         = this + "ON" + c
    def on(c: Option[Joinable]): Statement = c.map(on).getOrElse(this)

    def where(c: Joinable): Statement = this + "\nWHERE" + c

    def groupby(c: Joinable): Statement         = this + "\nGROUP BY" + c
    def groupby(c: Option[Joinable]): Statement = c.map(groupby).getOrElse(this)

    def limit(c: Joinable): Statement = this + "\nLIMIT" + c

    def orderby(c: Joinable): Statement         = this + "\nORDER BY" + c
    def orderby(c: Option[Joinable]): Statement = c.map(orderby).getOrElse(this)

    def output(o: Seq[Attribute], updateFromFieldMap: Boolean = true): Statement =
      copy(
        output = o.map(
          f => {
            val target = if (updateFromFieldMap) fieldMap.getOrElse(f.exprId, f) else f
            AttributeReference(target.name, f.dataType, f.nullable, f.metadata)(
              f.exprId,
              f.qualifier
            )
          }
        )
      )
  }

  // ----------------------------------
  // Primary Chunk Types
  // ----------------------------------

  trait SQLChunk extends Chunk {
    val sql: String
    override def toSQL(fieldMap: Map[ExprId, Attribute]): String = sql
  }

  case class Raw(override val sql: String) extends SQLChunk

  case class Ident(name: String) extends SQLChunk {
    override val sql: String = MemsqlDialect.quoteIdentifier(name)

    // it's not clear that we ever need to fully-qualify references since we do field renames with expr-ids
    // If this changes then you can change this code to something like this:
    // (and grab the qualifier when creating Ident)
    //      qualifier
    //        .map(q => s"${MemsqlDialect.quoteIdentifier(q)}.")
    //        .getOrElse("") + MemsqlDialect.quoteIdentifier(name)
  }

  case class Relation(
      rawOutput: Seq[Attribute],
      reader: MemsqlReader,
      name: String,
      toLogicalPlan: (Seq[AttributeReference], String, VariableList, Boolean) => LogicalPlan
  ) extends SQLChunk {

    val isFinal = reader.isFinal

    val output = rawOutput.map(
      a => AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId)
    )

    override val sql: String = {
      val indentedQuery = reader.query.replace("\n", "\n  ")
      val alias         = MemsqlDialect.quoteIdentifier(name)
      s"(\n  $indentedQuery\n) AS $alias"
    }

    def renameOutput: LogicalPlan =
      select(
        output
          .map(a => alias(a.name, a.name, a.exprId))
          .reduce(_ + "," + _))
        .from(this)
        .output(output)
        .asLogicalPlan()

    def castOutputAndFinalize: LogicalPlan = {
      val schema = try {
        reader.schema
      } catch {
        case e: Exception => {
          log.error(s"Failed to compute schema for reader:\n${reader.toString}")
          throw e
        }
      }

      val castedOutputExpr = output
        .zip(schema)
        .map({
          case (a, f) if a.dataType != f.dataType =>
            Alias(Cast(a, a.dataType), a.name)(a.exprId, a.qualifier, Some(a.metadata))

          case (a, _) => a
        })

      select(castedOutputExpr match {
        case Expression(expr) => expr
        case _                => None
      }).from(this)
        .output(output)
        .asLogicalPlan(true)
    }
  }

  object Relation {
    def unapply(source: LogicalPlan): Option[Relation] =
      source match {
        case LogicalRelation(reader: MemsqlReader, output, catalogTable, isStreaming) => {
          def convertBack(output: Seq[AttributeReference],
                          sql: String,
                          variables: VariableList,
                          isFinal: Boolean): LogicalPlan = {
            new LogicalRelation(
              reader.copy(query = sql,
                          variables = variables,
                          isFinal = isFinal,
                          expectedOutput = output),
              output,
              catalogTable,
              isStreaming
            )
          }

          Some(Relation(output, reader, aliasGen.next, convertBack))
        }
        case _ => None
      }
  }

  case class Attr(a: Attribute) extends Chunk {
    override def toSQL(fieldMap: Map[ExprId, Attribute]): String = {
      val target = fieldMap.getOrElse(a.exprId, a)
      Ident(s"${target.name}#${target.exprId.id}").sql
    }
  }

  // ----------------------------------
  // Variables
  // ----------------------------------

  sealed trait Var[T] extends SQLChunk {
    override val sql: String = "?"
    val variable: T
  }
  case class StringVar(override val variable: String)       extends Var[String]
  case class IntVar(override val variable: Int)             extends Var[Int]
  case class LongVar(override val variable: Long)           extends Var[Long]
  case class ShortVar(override val variable: Short)         extends Var[Short]
  case class FloatVar(override val variable: Float)         extends Var[Float]
  case class DoubleVar(override val variable: Double)       extends Var[Double]
  case class DecimalVar(override val variable: Decimal)     extends Var[Decimal]
  case class BooleanVar(override val variable: Boolean)     extends Var[Boolean]
  case class ByteVar(override val variable: Byte)           extends Var[Byte]
  case class DateVar(override val variable: Date)           extends Var[Date]
  case class TimestampVar(override val variable: Timestamp) extends Var[Timestamp]

  // ----------------------------------
  // Builder functions and constants
  // ----------------------------------

  final val empty: Statement = Statement(Nil)

  implicit def stringToJoinable(s: String): Joinable = Raw(s)

  def block(j: Joinable): Statement = Raw("(") + j + ")"

  def alias(j: Joinable, n: String, e: ExprId): Statement =
    block(j) + "AS" + Ident(s"${n}#${e.id}")

  def func(n: String, j: Joinable): Statement  = Raw(n) + block(j)
  def func(n: String, j: Joinable*): Statement = Raw(n) + block(j.reduce(_ + "," + _))

  def cast(j: Joinable, t: Joinable): Statement = func("CONVERT", j, t)

  def newStatement(sourcePlan: LogicalPlan): Statement = empty.withLogicalPlanComment(sourcePlan)

  def selectAll: Statement                   = Statement(Raw("SELECT *") :: Nil)
  def select(c: Joinable): Statement         = Raw("SELECT") + c
  def select(c: Option[Joinable]): Statement = Raw("SELECT") + c.getOrElse(Raw("*"))

  val fromLogicalPlan: PartialFunction[LogicalPlan, Statement] = {

    case plan @ Project(Expression(expr), Relation(relation)) =>
      newStatement(plan)
        .select(expr)
        .from(relation)
        .output(plan.output)

    case plan @ Filter(Expression(expr), Relation(relation)) =>
      newStatement(plan)
        .selectAll()
        .from(relation)
        .where(expr)
        .output(plan.output)

    case plan @ Aggregate(Expression(groupingExpr),
                          Expression(aggregateExpr),
                          Relation(relation)) =>
      newStatement(plan)
        .select(aggregateExpr)
        .from(relation)
        .groupby(groupingExpr)
        .output(plan.output)

    case plan @ Window(Expression(windowExpressions), _, _, Relation(relation)) => {
      newStatement(plan)
        .select(windowExpressions.map(exp => Raw("*,") + exp))
        .from(relation)
        .output(plan.output)
    }

    case plan @ Join(Relation(left),
                     Relation(right),
                     joinType @ (Inner | Cross),
                     Expression(condition)) =>
      newStatement(plan)
        .selectAll()
        .from(left)
        .join(right, joinType)
        .on(condition)
        .output(plan.output)

    // condition is required for {Left, Right, Full} outer joins
    // TODO: need to verify that both relations are part of the same cluster
    case plan @ Join(Relation(left),
                     Relation(right),
                     joinType @ (LeftOuter | RightOuter | FullOuter),
                     Expression(Some(condition))) =>
      newStatement(plan)
        .selectAll()
        .from(left)
        .join(right, joinType)
        .on(condition)
        .output(plan.output)

    // condition is not allowed for natural joins
    case plan @ Join(Relation(left), Relation(right), NaturalJoin(joinType), None) =>
      newStatement(plan)
        .selectAll()
        .from(left)
        .join(right, joinType)
        .output(plan.output)

  }

  val fromSingleLimitSort: PartialFunction[LogicalPlan, Statement] = {

    case plan @ Limit(Expression(expr), Relation(relation)) =>
      newStatement(plan)
        .selectAll()
        .from(relation)
        .limit(expr)
        .output(plan.output)

    case plan @ Sort(Expression(expr), true, Relation(relation)) =>
      newStatement(plan)
        .selectAll()
        .from(relation)
        .orderby(expr)
        // For now - we add a huge limit to all sort queries which forces MemSQL to preserve the order by.
        // fromNestedLogicalPlan will handle pushing down sort without a max-int limit)
        .limit(Long.MaxValue.toString)
        .output(plan.output)

  }

  val fromNestedLogicalPlan: PartialFunction[LogicalPlan, Statement] = {

    case plan @ Limit(Expression(limitExpr),
                      innerPlan @ Project(Expression(projectExpr), Relation(relation))) =>
      newStatement(plan)
        .withLogicalPlanComment(innerPlan)
        .select(projectExpr)
        .from(relation)
        .limit(limitExpr)
        .output(plan.output)

    case plan @ Limit(Expression(limitExpr),
                      innerPlan1 @ Project(
                        Expression(projectExpr),
                        innerPlan2 @ Sort(Expression(sortExpr), true, Relation(relation)))) =>
      newStatement(plan)
        .withLogicalPlanComment(innerPlan1)
        .withLogicalPlanComment(innerPlan2)
        .select(projectExpr)
        .from(relation)
        .orderby(sortExpr)
        .limit(limitExpr)
        .output(plan.output)

    case plan @ Limit(Expression(limitExpr),
                      innerPlan @ Sort(Expression(sortExpr), true, Relation(relation))) =>
      newStatement(plan)
        .withLogicalPlanComment(innerPlan)
        .selectAll()
        .from(relation)
        .orderby(sortExpr)
        .limit(limitExpr)
        .output(plan.output)

    case plan @ Sort(Expression(sortExpr),
                     true,
                     innerPlan @ Limit(Expression(limitExpr), Relation(relation))) =>
      newStatement(plan)
        .withLogicalPlanComment(innerPlan)
        .selectAll()
        .from(relation)
        .orderby(sortExpr)
        .limit(limitExpr)
        .output(plan.output)

  }
}

object Expression {
  import SQLGen._
  protected lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  def unapply(arg: Expression): Option[Joinable] = {
    val out = ExpressionGen.apply.lift(arg)

    if (out.isEmpty) {
      log.debug(s"Warning: MemSQL SQL pushdown was unable to convert expression: ${arg.asCode}")
    }

    out
  }

  def unapply(arg: Option[Expression]): Option[Option[Joinable]] =
    arg.map(ExpressionGen.apply.lift)

  def unapply(args: Seq[Expression]): Option[Option[Joinable]] = {
    if (args.isEmpty) {
      Some(None)
    } else {
      if (args.lengthCompare(1) > 0) {
        val expressionNames = new mutable.HashSet[String]()
        val hasDuplicates = args.exists({
          case a @ NamedExpression(name, _) => !expressionNames.add(s"${name}#${a.exprId.id}")
          case _                            => false
        })
        if (hasDuplicates) return None
      }

      args
        .map(ExpressionGen.apply.lift)
        .reduce[Option[Joinable]] {
          case (Some(left), Some(right)) => Some(left + "," + right)
          case _                         => None
        }
        .map(j => Some(j))
    }
  }

}
