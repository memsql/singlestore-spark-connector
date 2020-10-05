package com.memsql.spark

import com.memsql.spark.SQLGen.{ExpressionExtractor, SQLGenContext, Statement}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateFunction,
  BitAndAgg,
  BitOrAgg,
  BitXorAgg
}

case class BitAggregateExpressionExtractor(expressionExtractor: ExpressionExtractor,
                                           context: SQLGenContext,
                                           filter: Option[SQLGen.Joinable]) {
  def unapply(f: AggregateFunction): Option[Statement] = {
    f match {
      // BitAnd.scala
      case BitAndAgg(expressionExtractor(child)) if context.memsqlVersionAtLeast("7.0.1") =>
        Some(ExpressionGen.aggregateWithFilter("BIT_AND", child, filter))
      // BitOr.scala
      case BitOrAgg(expressionExtractor(child)) if context.memsqlVersionAtLeast("7.0.1") =>
        Some(ExpressionGen.aggregateWithFilter("BIT_OR", child, filter))
      // BitXor.scala
      case BitXorAgg(expressionExtractor(child)) if context.memsqlVersionAtLeast("7.0.1") =>
        Some(ExpressionGen.aggregateWithFilter("BIT_XOR", child, filter))
      case _ => None
    }
  }
}
