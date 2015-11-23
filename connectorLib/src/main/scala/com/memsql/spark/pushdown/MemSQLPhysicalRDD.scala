package com.memsql.spark.pushdown

import com.memsql.spark.connector.dataframe.MemSQLDataFrame
import com.memsql.spark.connector.rdd.MemSQLRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.types.StructField
import StringBuilderImplicits._

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
  protected override def doExecute(): RDD[InternalRow] =
    RDDConversions.rowToRowRdd(rdd, output.map(_.dataType))

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
   * NOTE: The reason we need to map query.params to a Seq[AnyRef]
   *       is so that Scala knows they can be safely converted to Java Objects
   *
   * @return A MemSQLPhysicalRDD ready to pass back to Spark as part of a physical plan
   */
  def fromAbstractQueryTree(sparkContext: SparkContext, tree: AbstractQuery): MemSQLPhysicalRDD = {
    val query = tree.collapse(QueryAlias(prefix="pushdown"))
    val cxnInfo = tree.getConnectionInfo.orNull

    if (cxnInfo == null) {
      throw new MemSQLPushdownException("Query tree does not terminate with a valid BaseQuery instance.")
    }

    val sql = query.sql.toString()
    val sqlParams = query.params.map(_.asInstanceOf[AnyRef])

    val actualOutput = MemSQLDataFrame.getQuerySchema(cxnInfo, sql, sqlParams)
    val output: Seq[AttributeReference] = actualOutput.zip(tree.output).map {
      case (f: StructField, a: Attribute) =>
        AttributeReference(f.name, f.dataType, f.nullable, f.metadata)(a.exprId, a.qualifiers)
    }

    MemSQLPhysicalRDD(
      output,
      MemSQLDataFrame.MakeMemSQLRowRDD(sparkContext, cxnInfo, sql, sqlParams),
      tree
    )
  }
}

