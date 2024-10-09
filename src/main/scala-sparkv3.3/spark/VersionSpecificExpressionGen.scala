package com.singlestore.spark

import com.singlestore.spark.ExpressionGen._
import com.singlestore.spark.SQLGen.{
  ExpressionExtractor,
  IntVar,
  Joinable,
  Raw,
  StringVar,
  block,
  cast,
  sqlMapValueCaseInsensitive,
  stringToJoinable
}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{
  BinaryType,
  BooleanType,
  ByteType,
  DateType,
  DayTimeIntervalType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  NullType,
  ShortType,
  StringType,
  TimestampType,
  YearMonthIntervalType
}

case class VersionSpecificExpressionGen(expressionExtractor: ExpressionExtractor) {
  def unapply(e: Expression): Option[Joinable] = e match {
    case MakeDate(expressionExtractor(year),
                  expressionExtractor(month),
                  expressionExtractor(day),
                  false) =>
      Some(f("DATE", f("CONCAT", year, "'-'", month, "'-'", day)))
    case MakeTimestamp(expressionExtractor(year),
                       expressionExtractor(month),
                       expressionExtractor(day),
                       expressionExtractor(hour),
                       expressionExtractor(min),
                       expressionExtractor(sec),
                       _,
                       _,
                       false,
                       TimestampType) =>
      Some(
        f("TIMESTAMP",
          f("CONCAT", year, "'-'", month, "'-'", day, "' '", hour, "':'", min, "':'", sec)))

    case Elt(expressionExtractor(Some(child)), false) => Some(f("ELT", child))

    case IntegralDivide(expressionExtractor(left), expressionExtractor(right), false) =>
      Some(f("FLOOR", op("/", left, right)))

    // arithmetic.scala
    case Add(expressionExtractor(left), expressionExtractor(right), false) =>
      Some(op("+", left, right))
    case Subtract(expressionExtractor(left), expressionExtractor(right), false) =>
      Some(op("-", left, right))
    case Multiply(expressionExtractor(left), expressionExtractor(right), false) =>
      Some(op("*", left, right))
    case Divide(expressionExtractor(left), expressionExtractor(right), false) =>
      Some(op("/", left, right))
    case Remainder(expressionExtractor(left), expressionExtractor(right), false) =>
      Some(op("%", left, right))
    case Abs(expressionExtractor(child), false) => Some(f("ABS", child))

    case Pmod(expressionExtractor(left), expressionExtractor(right), false) =>
      Some(block(block(block(left + "%" + right) + "+" + right) + "%" + right))

    // SingleStore and spark support other date formats
    // UnixTime doesn't use format if time is already a dataType or TimestampType
    case ToUnixTimestamp(e @ expressionExtractor(timeExp), _, _, false) if e.dataType == DateType =>
      Some(f("UNIX_TIMESTAMP", timeExp))

    case ToUnixTimestamp(e @ expressionExtractor(timeExp), _, _, false)
        if e.dataType == TimestampType =>
      Some(f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0"))

    case UnixTimestamp(e @ expressionExtractor(timeExp), _, _, false) if e.dataType == DateType =>
      Some(f("UNIX_TIMESTAMP", timeExp))

    case UnixTimestamp(e @ expressionExtractor(timeExp), _, _, false)
        if e.dataType == TimestampType =>
      Some(f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0"))

    // regexpExpression.scala
    case RegExpReplace(expressionExtractor(subject),
                       expressionExtractor(regexp),
                       expressionExtractor(rep),
                       pos) if pos.foldable && pos.eval() == null =>
      Some(f("REGEXP_REPLACE", subject, regexp, rep, StringVar("g")))

    case RegExpReplace(expressionExtractor(subject),
                       expressionExtractor(regexp),
                       expressionExtractor(rep),
                       intFoldableExtractor(pos)) =>
      Some(
        f("CONCAT",
          f("LEFT", subject, IntVar(pos - 1)),
          f("REGEXP_REPLACE",
            f("RIGHT", subject, op("-", f("LENGTH", subject), IntVar(pos - 1))),
            regexp,
            rep,
            StringVar("g"))))

    case UnaryMinus(expressionExtractor(child), false) => Some(f("-", child))

    // numberFormatExpressions.scala
    case ToNumber(expressionExtractor(left), expressionExtractor(right)) =>
      Some(f("TO_NUMBER", left, right))

    // randomExpression.scala
    // TODO PLAT-5759
    //
    // Need to match on both true & false that's why we use the wildcard here
    case Rand(expressionExtractor(child), _) => Some(f("RAND", child))

    // Cast.scala
    case Cast(e @ expressionExtractor(child), dataType, _, false) => {
      dataType match {
        case TimestampType => {
          e.dataType match {
            case _: BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
                DoubleType | _: DecimalType =>
              Some(cast(f("FROM_UNIXTIME", child), "DATETIME(6)"))
            case _ => Some(cast(child, "DATETIME(6)"))
          }
        }
        case DateType => Some(cast(child, "DATE"))

        case StringType => Some(cast(child, "CHAR"))
        case BinaryType => Some(cast(child, "BINARY"))

        case _: BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
            DoubleType | _: DecimalType =>
          if (e.dataType == DateType) {
            Some(StringVar(null))
          } else {
            val numeric_ch = if (e.dataType == TimestampType) {
              f("UNIX_TIMESTAMP", child)
            } else {
              child
            }
            dataType match {
              case BooleanType     => Some(op("!=", numeric_ch, IntVar(0)))
              case ByteType        => Some(op("!:>", numeric_ch, "TINYINT"))
              case ShortType       => Some(op("!:>", numeric_ch, "SMALLINT"))
              case IntegerType     => Some(op("!:>", numeric_ch, "INT"))
              case LongType        => Some(op("!:>", numeric_ch, "BIGINT"))
              case FloatType       => Some(op("!:>", numeric_ch, "FLOAT"))
              case DoubleType      => Some(op("!:>", numeric_ch, "DOUBLE"))
              case dt: DecimalType => Some(makeDecimal(numeric_ch, dt.precision, dt.scale))
            }
          }
        // SingleStore doesn't know how to handle this cast, pass it through AS is
        case _ => Some(child)
      }
    }

    // datetimeExpressions.scala

    case ConvertTimezone(expressionExtractor(sourceTz),
                         expressionExtractor(targetTz),
                         expressionExtractor(sourceTs)) =>
      Some(f("CONVERT_TZ", sourceTs, sourceTz, targetTz))

    case DateFromUnixDate(expressionExtractor(child)) =>
      Some(f("DATE", f("ADDDATE", Raw("FROM_UNIXTIME(0)"), Raw("INTERVAL") + child + Raw("DAY"))))
    case UnixDate(expressionExtractor(child)) =>
      Some(f("TIMESTAMPDIFF", "DAY", "'1970-01-01'", child))
    case UnixSeconds(expressionExtractor(child)) =>
      Some(f("TIMESTAMPDIFF", "SECOND", "'1970-01-01 00:00:00'", child))
    case UnixMicros(expressionExtractor(child)) =>
      Some(f("TIMESTAMPDIFF", "MICROSECOND", "'1970-01-01 00:00:00'", child))
    case UnixMillis(expressionExtractor(child)) =>
      Some(
        f("ROUND",
          op("/", f("TIMESTAMPDIFF", "MICROSECOND", "'1970-01-01 00:00:00'", child), "1000")))
    case SecondsToTimestamp(expressionExtractor(child)) =>
      Some(f("TIMESTAMPADD", "SECOND", child, "'1970-01-01 00:00:00'"))
    case MillisToTimestamp(expressionExtractor(child)) =>
      Some(f("TIMESTAMPADD", "MICROSECOND", op("*", child, "1000"), "'1970-01-01 00:00:00'"))
    case MicrosToTimestamp(expressionExtractor(child)) =>
      Some(f("TIMESTAMPADD", "MICROSECOND", child, "'1970-01-01 00:00:00'"))

    case LengthOfJsonArray(expressionExtractor(child)) =>
      Some(f("LENGTH", f("JSON_TO_ARRAY", child)))

    case NextDay(expressionExtractor(startDate), utf8StringFoldableExtractor(dayOfWeek), false) =>
      Some(
        computeNextDay(
          startDate,
          sqlMapValueCaseInsensitive(
            StringVar(dayOfWeek.toString),
            DAYS_OF_WEEK_OFFSET_MAP,
            StringVar(null)
          )
        ))

    case NextDay(expressionExtractor(startDate), expressionExtractor(dayOfWeek), false) =>
      Some(
        computeNextDay(startDate,
                       sqlMapValueCaseInsensitive(
                         dayOfWeek,
                         DAYS_OF_WEEK_OFFSET_MAP,
                         StringVar(null)
                       )))

    case TimeAdd(expressionExtractor(start),
                 Literal(v: Long, DayTimeIntervalType(_, _)),
                 timeZoneId) => {
      Some(addMicroseconds(start, v))
    }

    case TimestampAddYMInterval(expressionExtractor(start),
                                Literal(v: Int, YearMonthIntervalType(_, _)),
                                timeZoneId) => {
      Some(addMonths(start, v))
    }

    case Lead(expressionExtractor(input),
              expressionExtractor(offset),
              Literal(null, NullType),
              false) =>
      Some(f("LEAD", input, offset))
    case Lag(expressionExtractor(input),
             expressionExtractor(offset),
             Literal(null, NullType),
             false) =>
      Some(f("LAG", input, offset))

    case NthValue(expressionExtractor(input),
                  expressionExtractor(offset),
                  false) =>
      Some(f("NTH_VALUE", input, offset))

    case BitwiseGet(expressionExtractor(left), expressionExtractor(right)) =>
      Some(op("&", op(">>", left, right), "1"))

    case LikeAny(expressionExtractor(child), patterns) if patterns.size > 0 => {
      Some(likePatterns(child, patterns, "OR"))
    }
    case NotLikeAny(expressionExtractor(child), patterns) if patterns.size > 0 => {
      Some(f("NOT", likePatterns(child, patterns, "AND")))
    }
    case LikeAll(expressionExtractor(child), patterns) if patterns.size > 0 => {
      Some(likePatterns(child, patterns, "AND"))
    }
    case NotLikeAll(expressionExtractor(child), patterns) if patterns.size > 0 => {
      Some(f("NOT", likePatterns(child, patterns, "OR")))
    }

    case WidthBucket(expressionExtractor(value),
                     expressionExtractor(minValue),
                     expressionExtractor(maxValue),
                     expressionExtractor(numBucket)) => {
      var caseBranches = stringToJoinable("")
      // when (numBucket <= 0) or (minValue = maxValue) then null
      caseBranches += Raw("WHEN") + op(
        "|",
        op("<=", numBucket, IntVar(0)),
        op("=", minValue, maxValue),
      ) + Raw("THEN ") + StringVar(null)

      // when (value < minValue and minValue < maxValue) or (value > minValue and minValue > maxValue) then 0
      caseBranches += Raw("WHEN") + op(
        "|",
        op("&", op("<", value, minValue), op("<", minValue, maxValue)),
        op("&", op(">", value, minValue), op(">", minValue, maxValue))
      ) + Raw("THEN 0")

      // when (value > maxValue and minValue < maxValue) or (value < maxValue and minValue > maxValue) then numBucket + 1
      caseBranches += Raw("WHEN") + op(
        "|",
        op("&", op(">", value, maxValue), op("<", minValue, maxValue)),
        op("&", op("<", value, maxValue), op(">", minValue, maxValue))) +
        Raw("THEN") + op("+", numBucket, "1")

      // else FLOOR( (value - minValue)*numBucket / (maxValue - minValue) ) + 1 END
      val elseBranch = Raw("ELSE") + op(
        "+",
        f("FLOOR",
          op("/", op("*", numBucket, op("-", value, minValue)), op("-", maxValue, minValue))),
        IntVar(1)
      )
      Some(block(Raw("CASE") + caseBranches + elseBranch + Raw("END")))
    }

    // stringExpressions.scala
    case Left(expressionExtractor(str), expressionExtractor(len)) =>
      Some(f("LEFT", str, len))
    case Right(expressionExtractor(str), expressionExtractor(len)) =>
      Some(f("RIGHT", str, len))

    case Base64(expressionExtractor(child))   => Some(f("TO_BASE64", child))
    case UnBase64(expressionExtractor(child)) => Some(f("FROM_BASE64", child))

    case Round(expressionExtractor(child), expressionExtractor(scale)) => Some(f("ROUND", child, scale))
    case Unhex(expressionExtractor(child)) => Some(f("UNHEX", child))

    // ----------------------------------
    // Ternary Expressions
    // ----------------------------------

    // mathExpressions.scala
    case Sec(expressionExtractor(child)) => Some(op("/", "1", f("COS", child)))

    case Conv(expressionExtractor(numExpr),
              intFoldableExtractor(fromBase),
              intFoldableExtractor(toBase))
        // SingleStore supports bases only from [2, 36]
        if fromBase >= 2 && fromBase <= 36 &&
          toBase >= 2 && toBase <= 36 =>
      Some(f("CONV", numExpr, IntVar(fromBase), IntVar(toBase)))

    case _ => None
  }
}
