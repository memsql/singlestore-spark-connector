package com.memsql.spark

import com.memsql.spark.SQLGen.{ExpressionExtractor, SQLGenContext, Statement}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

case class BitAggregateExpressionExtractor(expressionExtractor: ExpressionExtractor,
                                           context: SQLGenContext,
                                           filter: Option[SQLGen.Joinable]) {
  def unapply(f: AggregateFunction): Option[Statement] = None
}
