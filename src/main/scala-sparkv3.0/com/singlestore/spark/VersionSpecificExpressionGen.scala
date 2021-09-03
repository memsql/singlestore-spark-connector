package com.singlestore.spark

import com.singlestore.spark.ExpressionGen._
import com.singlestore.spark.SQLGen.{ExpressionExtractor, IntVar, Joinable, Raw, StringVar, block}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{CalendarIntervalType, DateType, TimestampType}
import org.apache.spark.unsafe.types.CalendarInterval

case class VersionSpecificExpressionGen(expressionExtractor: ExpressionExtractor) {
  def unapply(e: Expression): Option[Joinable] = e match {
    case TimeSub(expressionExtractor(start),
                 Literal(v: CalendarInterval, CalendarIntervalType),
                 timeZoneId) => {
      def subDays(start: Joinable) =
        if (v.days == 0) {
          start
        } else {
          f("DATE_SUB", start, Raw("INTERVAL") + v.days.toString + "DAY")
        }

      Some(subMicroseconds(subDays(subMonths(start, v)), v))
    }

    case MakeDate(expressionExtractor(year),
                  expressionExtractor(month),
                  expressionExtractor(day)) =>
      Some(f("DATE", f("CONCAT", year, "'-'", month, "'-'", day)))
    case MakeTimestamp(expressionExtractor(year),
                       expressionExtractor(month),
                       expressionExtractor(day),
                       expressionExtractor(hour),
                       expressionExtractor(min),
                       expressionExtractor(sec),
                       _,
                       _) =>
      Some(
        f("TIMESTAMP",
          f("CONCAT", year, "'-'", month, "'-'", day, "' '", hour, "':'", min, "':'", sec)))

    case Elt(expressionExtractor(Some(child))) => Some(f("ELT", child))

    case IntegralDivide(expressionExtractor(left), expressionExtractor(right)) =>
      Some(f("FLOOR", op("/", left, right)))

    // arithmetic.scala
    case Add(expressionExtractor(left), expressionExtractor(right)) =>
      Some(op("+", left, right))
    case Subtract(expressionExtractor(left), expressionExtractor(right)) =>
      Some(op("-", left, right))
    case Multiply(expressionExtractor(left), expressionExtractor(right)) =>
      Some(op("*", left, right))
    case Divide(expressionExtractor(left), expressionExtractor(right)) =>
      Some(op("/", left, right))
    case Remainder(expressionExtractor(left), expressionExtractor(right)) =>
      Some(op("%", left, right))

    case Pmod(expressionExtractor(left), expressionExtractor(right)) =>
      Some(block(block(block(left + "%" + right) + "+" + right) + "%" + right))

    // SingleStore and spark support other date formats
    // UnixTime doesn't use format if time is already a dataType or TimestampType
    case ToUnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == DateType =>
      Some(f("UNIX_TIMESTAMP", timeExp))

    case ToUnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == TimestampType =>
      Some(f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0"))

    case UnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == DateType =>
      Some(f("UNIX_TIMESTAMP", timeExp))

    case UnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == TimestampType =>
      Some(f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0"))

    // regexpExpression.scala
    case RegExpReplace(expressionExtractor(subject),
                       expressionExtractor(regexp),
                       expressionExtractor(rep)) =>
      Some(f("REGEXP_REPLACE", subject, regexp, rep, StringVar("g")))

    case RegExpReplace(expressionExtractor(subject),
                       expressionExtractor(regexp),
                       expressionExtractor(rep)) =>
      Some(f("REGEXP_REPLACE", subject, regexp, rep, StringVar("g")))

    case UnaryMinus(expressionExtractor(child)) => Some(f("-", child))

    // randomExpression.scala
    // TODO PLAT-5759
    case Rand(expressionExtractor(child)) => Some(f("RAND", child))

    case _ => None
  }
}
