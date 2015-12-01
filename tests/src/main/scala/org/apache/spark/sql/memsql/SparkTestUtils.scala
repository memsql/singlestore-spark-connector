package org.apache.spark.sql.memsql

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

object SparkTestUtils {
  def exprToColumn(newExpr: Expression): Column = new Column(newExpr)
}
