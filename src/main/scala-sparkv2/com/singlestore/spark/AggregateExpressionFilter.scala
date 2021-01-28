package com.singlestore.spark

import com.singlestore.spark.SQLGen.ExpressionExtractor
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression

object AggregateExpressionFilter {
  def get(expressionExtractor: ExpressionExtractor,
          arg: AggregateExpression): Option[Option[SQLGen.Joinable]] = Some(None)
}
