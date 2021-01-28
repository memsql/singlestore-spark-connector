package com.singlestore.spark

import com.singlestore.spark.ExpressionGen.{
  f,
  addMicroseconds,
  addMonths,
  subMicroseconds,
  subMonths,
  longToDecimal,
  like
}
import com.singlestore.spark.SQLGen.{ExpressionExtractor, Joinable}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.CalendarIntervalType
import org.apache.spark.unsafe.types.CalendarInterval

case class VersionSpecificExpressionGen(expressionExtractor: ExpressionExtractor) {
  def unapply(e: Expression): Option[Joinable] = e match {
    case TimeAdd(expressionExtractor(start),
                 Literal(v: CalendarInterval, CalendarIntervalType),
                 timeZoneId) =>
      Some(addMicroseconds(addMonths(start, v), v))

    case TimeSub(expressionExtractor(start),
                 Literal(v: CalendarInterval, CalendarIntervalType),
                 timeZoneId) =>
      Some(subMicroseconds(subMonths(start, v), v))

    case Like(expressionExtractor(left), expressionExtractor(right)) =>
      Some(like(left, right))

    // decimalExpressions.scala
    // MakeDecimal and UnscaledValue value are used in DecimalAggregates optimizer
    // This optimizer replace Decimals inside of the sum and aggregate expressions to the Longs using UnscaledValue
    // and then casts the result back to Decimal using MakeDecimal
    case MakeDecimal(expressionExtractor(child), p, s) =>
      Some(longToDecimal(child, p, s))

    case _ => None
  }
}
