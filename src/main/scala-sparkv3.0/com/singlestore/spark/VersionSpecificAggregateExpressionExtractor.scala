package com.singlestore.spark

import com.singlestore.spark.SQLGen.{ExpressionExtractor, SQLGenContext, Statement}
import com.singlestore.spark.ExpressionGen.aggregateWithFilter
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateFunction,
  First,
  Last,
  StddevPop,
  StddevSamp,
  VariancePop,
  VarianceSamp
}
import org.apache.spark.sql.types.BooleanType

case class VersionSpecificAggregateExpressionExtractor(expressionExtractor: ExpressionExtractor,
                                                       context: SQLGenContext,
                                                       filter: Option[SQLGen.Joinable]) {
  def unapply(f: AggregateFunction): Option[Statement] = {
    f match {
      // CentralMomentAgg.scala
      case StddevPop(expressionExtractor(child)) =>
        Some(aggregateWithFilter("STDDEV_POP", child, filter))
      case StddevSamp(expressionExtractor(child)) =>
        Some(aggregateWithFilter("STDDEV_SAMP", child, filter))
      case VariancePop(expressionExtractor(child)) =>
        Some(aggregateWithFilter("VAR_POP", child, filter))
      case VarianceSamp(expressionExtractor(child)) =>
        Some(aggregateWithFilter("VAR_SAMP", child, filter))

      // First.scala
      case First(expressionExtractor(child), Literal(false, BooleanType)) =>
        Some(aggregateWithFilter("ANY_VALUE", child, filter))

      // Last.scala
      case Last(expressionExtractor(child), Literal(false, BooleanType)) =>
        Some(aggregateWithFilter("ANY_VALUE", child, filter))

      case _ => None
    }
  }
}
