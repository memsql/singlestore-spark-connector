package com.singlestore.spark

import com.singlestore.spark.SQLGen.{ExpressionExtractor, SQLGenContext, Statement}
import com.singlestore.spark.ExpressionGen.aggregateWithFilter
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateFunction,
  Average,
  First,
  Last,
  StddevPop,
  StddevSamp,
  Sum,
  VariancePop,
  VarianceSamp
}

case class VersionSpecificAggregateExpressionExtractor(expressionExtractor: ExpressionExtractor,
                                                       context: SQLGenContext,
                                                       filter: Option[SQLGen.Joinable]) {
  def unapply(f: AggregateFunction): Option[Statement] = {
    f match {
      // CentralMomentAgg.scala
      case StddevPop(expressionExtractor(child), true) =>
        Some(aggregateWithFilter("STDDEV_POP", child, filter))
      case StddevSamp(expressionExtractor(child), true) =>
        Some(aggregateWithFilter("STDDEV_SAMP", child, filter))
      case VariancePop(expressionExtractor(child), true) =>
        Some(aggregateWithFilter("VAR_POP", child, filter))
      case VarianceSamp(expressionExtractor(child), true) =>
        Some(aggregateWithFilter("VAR_SAMP", child, filter))

      // First.scala
      case First(expressionExtractor(child), false) =>
        Some(aggregateWithFilter("ANY_VALUE", child, filter))

      // Last.scala
      case Last(expressionExtractor(child), false) =>
        Some(aggregateWithFilter("ANY_VALUE", child, filter))

      // Sum.scala
      case Sum(expressionExtractor(child)) =>
        Some(aggregateWithFilter("SUM", child, filter))

      // Average.scala
      case Average(expressionExtractor(child)) =>
        Some(aggregateWithFilter("AVG", child, filter))

      case _ => None
    }
  }
}
