package com.memsql.spark.pushdown

import java.sql.{Date, Timestamp}
import java.math.BigDecimal

import scala.concurrent.duration._
import com.memsql.spark.connector.rdd.MemSQLRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.types._
import StringBuilderImplicits._
import com.memsql.spark.connector.util.JDBCImplicits._

/**
 * MemSQLPhysicalRDD represents a node in the final plan we pass back to Spark.
 * Effectively this node internalizes a RDD (which has already been created,
 * just not yet evaluated) which it returns to Spark when the node is
 * "executed".
 * @param rdd The RDD to internalize
 */
case class MemSQLPhysicalRDD(output: Seq[Attribute],
                             rdd: MemSQLRDD[Row],
                             @transient tree: AbstractQuery) extends SparkPlan {
  /**
   * This node is a leaf without children.
   */
  override def children: Seq[SparkPlan] = Nil

  /**
   * This method is called by Spark during execution.
   * We have already produced the RDD during pushdown
   * so we just return the RDD here.
   * @return The RDD produced during pushdown
   */
  protected override def doExecute(): RDD[InternalRow] = {
    val dataTypes = output.map(_.dataType)
    val convertedRDD = rdd.map { row =>
      Row.fromSeq(row.toSeq.zip(dataTypes).map {
        case (nanos: Long, TimestampType) => new Timestamp(nanos)
        case (nanos: Long, DateType) => new Date(nanos.nanoseconds.toMillis)
        case (bool: Long, BooleanType) => bool == 1
        case (decimal: BigDecimal, DoubleType) => decimal.doubleValue()
        case (decimal: BigDecimal, FloatType) => decimal.floatValue()
        case (r, _) => r
      })
    }
    RDDConversions.rowToRowRdd(convertedRDD, output.map(_.dataType))
  }

  /**
   * By overriding this method, the Spark application UI will show
   * the full SQL query in the optimized plan output.
   */
  override protected def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder =
    builder
      .indent(depth)
      .append(s"MemSQLPhysicalRDD[${rdd.sql}]\n")
      .append(tree.prettyPrint(depth + 1, builder))
}

/**
 * Convenience methods for creating a MemSQLPhysicalRDD
 */
object MemSQLPhysicalRDD {

  /**
   * Create a MemSQLPhysicalRDD from a PushdownState object.
   *
   * @return A MemSQLPhysicalRDD ready to pass back to Spark as part of a physical plan
   */
  def fromAbstractQueryTree(sparkContext: SparkContext, tree: AbstractQuery): MemSQLPhysicalRDD = {
    val treeOutput = tree.castedNamedOutput
    val query = new SQLBuilder()
      .raw("SELECT ")
      .addExpressions(treeOutput, ", ")
      .raw(" FROM ")
      .appendBuilder(tree.collapse)

    val sql = query.sql.toString()
    val sqlParams = query.params

    val baseQuery = tree.find { case q: BaseQuery => q }.orNull
    if (baseQuery == null) {
      throw new MemSQLPushdownException("Query tree does not terminate with a valid BaseQuery instance.")
    }

    val rdd = MemSQLRDD(
      sparkContext,
      baseQuery.cluster,
      sql,
      sqlParams,
      baseQuery.database,
      _.toRow)

    MemSQLPhysicalRDD(treeOutput.map(_.toAttribute), rdd, tree)
  }
}

