package com.memsql.spark

import com.memsql.spark.ExpressionGen.{
  f,
  op,
  addMonths,
  addMicroseconds,
  subMicroseconds,
  subMonths,
  longToDecimal
}
import com.memsql.spark.SQLGen.{ExpressionExtractor, Joinable, Raw}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.CalendarIntervalType
import org.apache.spark.unsafe.types.CalendarInterval

case class VersionSpecificExpressionGen(expressionExtractor: ExpressionExtractor) {
  def unapply(e: Expression): Option[Joinable] = e match {
    case TimeAdd(expressionExtractor(start),
                 Literal(v: CalendarInterval, CalendarIntervalType),
                 timeZoneId) => {
      def addDays(start: Joinable) =
        if (v.days == 0) {
          start
        } else {
          f("DATE_ADD", start, Raw("INTERVAL") + v.days.toString + "DAY")
        }

      Some(addMicroseconds(addDays(addMonths(start, v)), v))
    }

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

    case IntegralDivide(expressionExtractor(left), expressionExtractor(right)) =>
      Some(f("FLOOR", op("/", left, right)))

    case Like(expressionExtractor(left), expressionExtractor(right), escapeChar: Char) =>
      if (escapeChar == '\\') {
        Some(op("LIKE", left, right))
      } else {
        Some(op("LIKE", left, f("REPLACE", right, "'" + escapeChar.toString + "'", "'\\\\'")))
      }

    case BitwiseCount(expressionExtractor(child)) =>
      Some(f("BIT_COUNT", child))

    //    case DatePart(expressionExtractor(field), expressionExtractor(source), expressionExtractor(child)) => // Converts to CAST(field)
    //    case Extract(expressionExtractor(field), expressionExtractor(source), expressionExtractor(child))  => // Converts to CAST(field)
    case MakeDate(expressionExtractor(year),
                  expressionExtractor(month),
                  expressionExtractor(day)) =>
      Some(f("DATE", f("CONCAT", year, "'-'", month, "'-'", day)))
    //    case MakeInterval(_, _, _, _, _, _, _) => ???
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

    // MakeDecimal and UnscaledValue value are used in DecimalAggregates optimizer
    // This optimizer replace Decimals inside of the sum and aggregate expressions to the Longs using UnscaledValue
    // and then casts the result back to Decimal using MakeDecimal
    case MakeDecimal(expressionExtractor(child), p, s, _) =>
      Some(longToDecimal(child, p, s))

    // asinh(x) = ln(x + sqrt(x^2 + 1))
    case Asinh(expressionExtractor(child)) =>
      Some(f("LN", op("+", child, f("SQRT", op("+", f("POW", child, "2"), "1")))))

    // acosh(x) = ln(x + sqrt(x^2 - 1))
    case Acosh(expressionExtractor(child)) =>
      Some(f("LN", op("+", child, f("SQRT", op("-", f("POW", child, "2"), "1")))))

    // atanh(x) = 1/2 * ln((1 + x)/(1 - x))
    case Atanh(expressionExtractor(child)) =>
      Some(op("/", f("LN", op("/", op("+", "1", child), op("-", "1", child))), "2"))

    case WeekDay(expressionExtractor(child)) => Some(f("WEEKDAY", child))

    case _ => None
  }
}
