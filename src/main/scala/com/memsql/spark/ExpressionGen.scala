package com.memsql.spark

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object ExpressionGen extends LazyLogging {
  import SQLGen._

  final val MEMSQL_DECIMAL_MAX_PRECISION = 65
  final val MEMSQL_DECIMAL_MAX_SCALE     = 30
  final val MEMSQL_DEFAULT_TIME_FORMAT   = UTF8String.fromString("yyyy-MM-dd HH:mm:ss")

  // DAYS_OF_WEEK_OFFSET_MAP is a map from week day prefix to it's offset (sunday -> 1, saturday -> 7)
  final val DAYS_OF_WEEK_OFFSET_MAP: Map[String, String] = {
    val daysOfWeek =
      List("sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday")
    val prefix2    = daysOfWeek.map(day => day.slice(0, 2))
    val prefix3    = daysOfWeek.map(day => day.slice(0, 3))
    val dayFormats = daysOfWeek ::: prefix3 ::: prefix2

    dayFormats.zipWithIndex.map { case (day, ind) => (day, (ind % 7 + 1).toString) }.toMap
  }

  // helpers to keep this code sane
  def f(n: String, c: Joinable*)              = func(n, c: _*)
  def op(o: String, l: Joinable, r: Joinable) = block(l + o + r)

  def makeDecimal(child: Joinable, precision: Int, scale: Int): Joinable = {
    val p = Math.min(MEMSQL_DECIMAL_MAX_PRECISION, precision)
    val s = Math.min(MEMSQL_DECIMAL_MAX_SCALE, scale)
    cast(child, s"DECIMAL($p, $s)")
  }

  // computeNextDay returns a statement for computing the next date after startDate with specified offset (sunday -> 1, saturday -> 7)
  // ADDDATE(startDate,(dayOfWeekOffset - DAYOFWEEK(startDate) + 6)%7 +1)
  def computeNextDay(startDate: Joinable, offset: Joinable): Statement = f(
    "ADDDATE",
    startDate,
    op(
      "+",
      op(
        "%",
        op(
          "+",
          op(
            "-",
            offset,
            f("DAYOFWEEK", startDate)
          ),
          "6"
        ),
        "7"
      ),
      "1"
    )
  )

  object GenLiteral {
    def unapply(arg: Expression): Option[Joinable] = arg match {
      case Literal(v, _) if v == null      => Some(StringVar(null))
      case Literal(v: Int, DateType)       => Some(DateVar(DateTimeUtils.toJavaDate(v)))
      case Literal(v: Long, TimestampType) => Some(TimestampVar(DateTimeUtils.toJavaTimestamp(v)))

      case Literal(v, _) => convertLiteralValue.lift(v)

      case _ => None
    }

    def unapply(vals: Iterable[Any]): Option[Joinable] =
      vals
        .map(convertLiteralValue.lift)
        .reduce[Option[Joinable]] {
          case (Some(left), Some(right)) => Some(left + "," + right)
          case _                         => None
        }

    def convertLiteralValue: PartialFunction[Any, Joinable] = {
      case v if v == null => StringVar(null)

      case v: String     => StringVar(v)
      case v: UTF8String => StringVar(v.toString)
      case v: Byte       => ByteVar(v)

      case v: Boolean => Raw(if (v) "TRUE" else "FALSE")

      // MemSQL does not support intervals which define month + remainder at the same time
      case v: CalendarInterval if v.months == 0 =>
        Raw("INTERVAL") + v.microseconds.toString + "MICROSECOND"
      case v: CalendarInterval if v.months != 0 && v.microseconds == 0 =>
        Raw("INTERVAL") + v.months.toString + "MONTH"

      case v: Short                                  => Raw(v.toString)
      case v: Int                                    => Raw(v.toString)
      case v: Integer                                => Raw(v.toString)
      case v: Long                                   => Raw(v.toString)
      case v: Decimal                                => makeDecimal(Raw(v.toString), v.precision, v.scale)
      case v: BigDecimal                             => makeDecimal(Raw(v.toString), v.precision, v.scale)
      case v: Float if java.lang.Float.isFinite(v)   => Raw(v.toString)
      case v: Double if java.lang.Double.isFinite(v) => Raw(v.toString)
    }
  }

  case class DecimalExpressionExtractor(expressionExtractor: ExpressionExtractor) {
    def unapply(e: Expression): Option[(Joinable, Int, Int)] = (e, e.dataType) match {
      case (expressionExtractor(child), t: DecimalType) => Some((child, t.precision, t.scale))
      case _                                            => None
    }
  }

  case class WindowBoundaryExpressionExtractor(expressionExtractor: ExpressionExtractor) {
    def unapply(arg: Expression): Option[Joinable] = arg match {
      case e: SpecialFrameBoundary                => Some(e.sql)
      case UnaryMinus(expressionExtractor(child)) => Some(child + "PRECEDING")
      case Literal(n: Integer, IntegerType) =>
        Some(Raw(Math.abs(n).toString) + (if (n < 0) "PRECEDING" else "FOLLOWING"))
      case expressionExtractor(child) => Some(child + "FOLLOWING")
      case _                          => None
    }
  }

  // we need to manually unwrap MonthsBetween since the roundOff argument
  // does not exist in Spark 2.3
  // The roundOff argument truncates the result to 8 digits of precision
  // which we can safely ignore, the user can apply an explicit round if needed
  case class MonthsBetweenExpressionExtractor(expressionExtractor: ExpressionExtractor) {
    def unapply(arg: MonthsBetween): Option[(Joinable, Joinable)] =
      for {
        date1 <- expressionExtractor.unapply(arg.date1)
        date2 <- expressionExtractor.unapply(arg.date2)
      } yield (date1, date2)
  }

  def apply(expressionExtractor: ExpressionExtractor): PartialFunction[Expression, Joinable] = {
    val windowBoundaryExpressionExtractor = WindowBoundaryExpressionExtractor(expressionExtractor)
    val monthsBetweenExpressionExtractor  = MonthsBetweenExpressionExtractor(expressionExtractor)
    val decimalExpressionExtractor        = DecimalExpressionExtractor(expressionExtractor)
    return {
      // ----------------------------------
      // Attributes
      // ----------------------------------
      case a: Attribute => Attr(a, expressionExtractor.context)
      case a @ Alias(expressionExtractor(child), name) =>
        alias(child, name, a.exprId, expressionExtractor.context)

      // ----------------------------------
      // Literals
      // ----------------------------------
      case GenLiteral(v) => v

      // ----------------------------------
      // Variable Expressions
      // ----------------------------------

      case Coalesce(expressionExtractor(Some(child))) => f("COALESCE", child)
      case Least(expressionExtractor(Some(child)))    => f("LEAST", child)
      case Greatest(expressionExtractor(Some(child))) => f("GREATEST", child)
      case Concat(expressionExtractor(Some(child)))   => f("CONCAT", child)
      case Elt(expressionExtractor(Some(child)))      => f("ELT", child)

      // ----------------------------------
      // Aggregate Expressions
      // ----------------------------------

      // Average.scala
      case AggregateExpression(Average(expressionExtractor(child)), _, _, _) =>
        f("AVG", child)

      // CentralMomentAgg.scala
      case AggregateExpression(StddevPop(expressionExtractor(child)), _, _, _) =>
        f("STDDEV_POP", child)
      case AggregateExpression(StddevSamp(expressionExtractor(child)), _, _, _) =>
        f("STDDEV_SAMP", child)
      case AggregateExpression(VariancePop(expressionExtractor(child)), _, _, _) =>
        f("VAR_POP", child)
      case AggregateExpression(VarianceSamp(expressionExtractor(child)), _, _, _) =>
        f("VAR_SAMP", child)

      // TODO: case Skewness(expressionExtractor(child))     => ???
      // TODO: case Kurtosis(expressionExtractor(child))     => ???

      // Count.scala
      case AggregateExpression(Count(expressionExtractor(None)), _, false, _) =>
        Raw("COUNT(*)")
      case AggregateExpression(Count(expressionExtractor(Some(children))), _, isDistinct, _) =>
        if (isDistinct) {
          Raw("COUNT") + block(Raw("DISTINCT") + children)
        } else {
          f("COUNT", children)
        }

      // Covariance.scala
      // TODO: case CovPopulation(expressionExtractor(left), expressionExtractor(right)) => ???
      // TODO: case CovSample(expressionExtractor(left), expressionExtractor(right))     => ???

      // First.scala
      case AggregateExpression(First(expressionExtractor(child), Literal(false, BooleanType)),
                               _,
                               _,
                               _) =>
        f("ANY_VALUE", child)

      // Last.scala
      case AggregateExpression(Last(expressionExtractor(child), Literal(false, BooleanType)),
                               _,
                               _,
                               _) =>
        f("ANY_VALUE", child)

      // Max.scala
      case AggregateExpression(Max(expressionExtractor(child)), _, _, _) => f("MAX", child)

      // Min.scala
      case AggregateExpression(Min(expressionExtractor(child)), _, _, _) => f("MIN", child)

      // Sum.scala
      case AggregateExpression(Sum(expressionExtractor(child)), _, _, _) => f("SUM", child)

      // windowExpressions.scala
      case WindowExpression(expressionExtractor(child),
                            WindowSpecDefinition(expressionExtractor(partitionSpec),
                                                 expressionExtractor(orderSpec),
                                                 expressionExtractor(frameSpec))) =>
        child + "OVER" + block(
          partitionSpec.map(Raw("PARTITION BY") + _).getOrElse(empty) +
            orderSpec.map(Raw("ORDER BY") + _).getOrElse(empty) +
            frameSpec
        )

      case UnspecifiedFrame => ""

      case SpecifiedWindowFrame(frameType,
                                windowBoundaryExpressionExtractor(lower),
                                windowBoundaryExpressionExtractor(upper)) =>
        Raw(frameType.sql) + "BETWEEN" + lower + "AND" + upper

      case Lead(expressionExtractor(input), expressionExtractor(offset), Literal(null, NullType)) =>
        f("LEAD", input, offset)
      case Lag(expressionExtractor(input), expressionExtractor(offset), Literal(null, NullType)) =>
        f("LAG", input, offset)
      case RowNumber()                       => "ROW_NUMBER()"
      case NTile(expressionExtractor(child)) => f("NTILE", child)
      case Rank(_)                           => "RANK()"
      case DenseRank(_)                      => "DENSE_RANK()"
      case PercentRank(_)                    => "PERCENT_RANK()"

      // TODO: case CumeDist()               => ???

      // ----------------------------------
      // Binary Expressions
      // ----------------------------------

      // arithmetic.scala

      case Add(expressionExtractor(left), expressionExtractor(right))       => op("+", left, right)
      case Subtract(expressionExtractor(left), expressionExtractor(right))  => op("-", left, right)
      case Multiply(expressionExtractor(left), expressionExtractor(right))  => op("*", left, right)
      case Divide(expressionExtractor(left), expressionExtractor(right))    => op("/", left, right)
      case Remainder(expressionExtractor(left), expressionExtractor(right)) => op("%", left, right)

      case Pmod(expressionExtractor(left), expressionExtractor(right)) =>
        block(block(block(left + "%" + right) + "+" + right) + "%" + right)

      // bitwiseExpressions.scala
      case BitwiseAnd(expressionExtractor(left), expressionExtractor(right)) => op("&", left, right)
      case BitwiseOr(expressionExtractor(left), expressionExtractor(right))  => op("|", left, right)
      case BitwiseXor(expressionExtractor(left), expressionExtractor(right)) => op("^", left, right)

      // datetimeExpressions.scala

      // NOTE: we explicitly ignore the timeZoneId field in all of the following expressionExtractors
      // The user is required to setup Spark and/or MemSQL with the timezone they want or they
      // will get inconsistent results with/without pushdown.

      case DateAdd(expressionExtractor(startDate), expressionExtractor(days)) =>
        f("ADDDATE", startDate, days)
      case DateSub(expressionExtractor(startDate), expressionExtractor(days)) =>
        f("SUBDATE", startDate, days)
      case DateFormatClass(expressionExtractor(left), expressionExtractor(right), timeZoneId) =>
        f("DATE_FORMAT", left, right)

      // Special case since MemSQL doesn't support INTERVAL with both month and microsecond
      case TimeAdd(expressionExtractor(start),
                   Literal(v: CalendarInterval, CalendarIntervalType),
                   timeZoneId) if v.months > 0 && v.microseconds > 0 =>
        f(
          "DATE_ADD",
          f("DATE_ADD", start, Raw("INTERVAL") + v.months.toString + "MONTH"),
          Raw("INTERVAL") + v.microseconds.toString + "MICROSECOND"
        )

      case TimeAdd(expressionExtractor(start), expressionExtractor(interval), timeZoneId) =>
        f("DATE_ADD", start, interval)

      // Special case since MemSQL doesn't support INTERVAL with both month and microsecond
      case TimeSub(expressionExtractor(start),
                   Literal(v: CalendarInterval, CalendarIntervalType),
                   timeZoneId) if v.months > 0 && v.microseconds > 0 =>
        f("DATE_SUB",
          f("DATE_SUB", start, Raw("INTERVAL") + v.months.toString + "MONTH"),
          Raw("INTERVAL") + v.microseconds.toString + "MICROSECOND")

      case TimeSub(expressionExtractor(start), expressionExtractor(interval), timeZoneId) =>
        f("DATE_SUB", start, interval)

      case FromUTCTimestamp(expressionExtractor(timestamp), expressionExtractor(timezone)) =>
        f("CONVERT_TZ", timestamp, StringVar("UTC"), timezone)

      case ToUTCTimestamp(expressionExtractor(timestamp), expressionExtractor(timezone)) =>
        f("CONVERT_TZ", timestamp, timezone, StringVar("UTC"))

      case TruncTimestamp(expressionExtractor(format),
                          expressionExtractor(timestamp),
                          timeZoneId) => {
        f(
          "DATE_TRUNC",
          sqlMapValueCaseInsensitive(
            format,
            Map(
              // MemSQL doesn't support formats ("yyyy", "yy", "mon", "mm", "dd") so we map them here
              "yyyy" -> "year",
              "yy"   -> "year",
              "mon"  -> "month",
              "mm"   -> "month",
              "dd"   -> "day"
            ),
            format
          ),
          timestamp
        )
      }

      case TruncDate(expressionExtractor(date), expressionExtractor(format)) => {
        f(
          "DATE_TRUNC",
          sqlMapValueCaseInsensitive(
            format,
            Map(
              // MemSQL doesn't support formats ("yyyy", "yy", "mon", "mm") so we map them here
              "yyyy" -> "year",
              "yy"   -> "year",
              "mon"  -> "month",
              "mm"   -> "month"
            ),
            format
          ),
          date
        )
      }

      case monthsBetweenExpressionExtractor((date1, date2)) =>
        f("MONTHS_BETWEEN", date1, date2)

      case AddMonths(expressionExtractor(startDate), expressionExtractor(numMonths)) =>
        f("DATE_ADD", startDate, Raw("INTERVAL") + numMonths + "MONTH")

      // MemSQL and spark support other date formats
      // UnixTime doesn't use format if time is already a dataType or TimestampType
      case ToUnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == DateType =>
        f("UNIX_TIMESTAMP", timeExp)

      case ToUnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == TimestampType =>
        f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0")

      case UnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == DateType =>
        f("UNIX_TIMESTAMP", timeExp)

      case UnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == TimestampType =>
        f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0")

      case FromUnixTime(expressionExtractor(sec), format, timeZoneId)
          if format.foldable && format.dataType == StringType &&
            format.eval().asInstanceOf[UTF8String] == MEMSQL_DEFAULT_TIME_FORMAT =>
        f("FROM_UNIXTIME", sec)

      case NextDay(expressionExtractor(startDate), dayOfWeek)
          if dayOfWeek.foldable && dayOfWeek.dataType == StringType =>
        computeNextDay(
          startDate,
          sqlMapValueCaseInsensitive(
            StringVar(dayOfWeek.eval().asInstanceOf[UTF8String].toString),
            DAYS_OF_WEEK_OFFSET_MAP,
            StringVar(null)
          )
        )

      case NextDay(expressionExtractor(startDate), expressionExtractor(dayOfWeek)) =>
        computeNextDay(startDate,
                       sqlMapValueCaseInsensitive(
                         dayOfWeek,
                         DAYS_OF_WEEK_OFFSET_MAP,
                         StringVar(null)
                       ))

      case DateDiff(expressionExtractor(endDate), expressionExtractor(startDate)) =>
        f("DATEDIFF", endDate, startDate)

      // hash.scala
      case Sha2(expressionExtractor(left), expressionExtractor(right)) => f("SHA2", left, right)

      // mathExpressions.scala
      case Atan2(expressionExtractor(left), expressionExtractor(right))     => f("ATAN2", left, right)
      case Pow(expressionExtractor(left), expressionExtractor(right))       => f("POWER", left, right)
      case ShiftLeft(expressionExtractor(left), expressionExtractor(right)) => op("<<", left, right)
      case ShiftRight(expressionExtractor(left), expressionExtractor(right)) =>
        op(">>", left, right)
      case ShiftRightUnsigned(expressionExtractor(left), expressionExtractor(right)) =>
        op(">>", left, right)
      case Logarithm(expressionExtractor(left), expressionExtractor(right)) => f("LOG", left, right)
      case Round(expressionExtractor(child), expressionExtractor(scale))    => f("ROUND", child, scale)

      case Hypot(expressionExtractor(left), expressionExtractor(right)) =>
        f("SQRT", op("+", f("POW", left, "2"), f("POW", right, "2")))

      // TODO: case _: BRound => None

      // predicates.scala
      case And(expressionExtractor(left), expressionExtractor(right)) => op("AND", left, right)
      case Or(expressionExtractor(left), expressionExtractor(right))  => op("OR", left, right)

      case EqualTo(expressionExtractor(left), expressionExtractor(right)) => op("=", left, right)
      case EqualNullSafe(expressionExtractor(left), expressionExtractor(right)) =>
        op("<=>", left, right)
      case LessThan(expressionExtractor(left), expressionExtractor(right)) => op("<", left, right)
      case LessThanOrEqual(expressionExtractor(left), expressionExtractor(right)) =>
        op("<=", left, right)
      case GreaterThan(expressionExtractor(left), expressionExtractor(right)) =>
        op(">", left, right)
      case GreaterThanOrEqual(expressionExtractor(left), expressionExtractor(right)) =>
        op(">=", left, right)

      case In(expressionExtractor(child), expressionExtractor(Some(elements))) =>
        op("IN", child, block(elements))

      case InSet(expressionExtractor(child), GenLiteral(elements)) =>
        op(
          "IN",
          child,
          block(elements)
        )

      // regexpExpressions.scala
      case Like(expressionExtractor(left), expressionExtractor(right))  => op("LIKE", left, right)
      case RLike(expressionExtractor(left), expressionExtractor(right)) => op("RLIKE", left, right)

      // stringExpressions.scala
      case Contains(expressionExtractor(left), expressionExtractor(right)) =>
        op(">", f("INSTR", left, right), "0")
      case StartsWith(expressionExtractor(left), expressionExtractor(right)) =>
        op("LIKE", left, f("CONCAT", right, StringVar("%")))
      case EndsWith(expressionExtractor(left), expressionExtractor(right)) =>
        op("LIKE", left, f("CONCAT", StringVar("%"), right))
      case StringInstr(expressionExtractor(str), expressionExtractor(substr)) =>
        f("INSTR", str, substr)
      case FormatNumber(expressionExtractor(x), expressionExtractor(d)) => f("FORMAT", x, d)
      case StringRepeat(expressionExtractor(child), expressionExtractor(times)) =>
        f("LPAD", StringVar(""), times + "*" + f("CHAR_LENGTH", child), child)

      case StringTrim(expressionExtractor(srcStr), None) =>
        f("TRIM", Raw("BOTH") + "FROM" + srcStr)
      case StringTrim(expressionExtractor(srcStr), Some(trimStr))
          if trimStr.foldable && trimStr.dataType == StringType &&
            trimStr.eval().asInstanceOf[UTF8String] == UTF8String.fromString(" ") =>
        f("TRIM", Raw("BOTH") + "FROM" + srcStr)

      case StringTrimLeft(expressionExtractor(srcStr), None) =>
        f("LTRIM", srcStr)
      case StringTrimLeft(expressionExtractor(srcStr), Some(trimStr))
          if trimStr.foldable && trimStr.dataType == StringType &&
            trimStr.eval().asInstanceOf[UTF8String] == UTF8String.fromString(" ") =>
        f("LTRIM", srcStr)

      case StringTrimRight(expressionExtractor(srcStr), None) =>
        f("RTRIM", srcStr)
      case StringTrimRight(expressionExtractor(srcStr), Some(trimStr))
          if trimStr.foldable && trimStr.dataType == StringType &&
            trimStr.eval().asInstanceOf[UTF8String] == UTF8String.fromString(" ") =>
        f("RTRIM", srcStr)

      // TODO: case _: Levenshtein => None

      // ----------------------------------
      // Leaf Expressions
      // ----------------------------------

      // datetimeExpressions.scala
      case CurrentDate(_)     => "CURRENT_DATE()"
      case CurrentTimestamp() => "NOW(6)"

      // mathExpressions.scala
      case EulerNumber() => math.E.toString
      case Pi()          => "PI()"

      // ----------------------------------
      // Ternary Expressions
      // ----------------------------------

      // mathExpressions.scala
      case Conv(expressionExtractor(numExpr),
                expressionExtractor(fromBaseExpr),
                expressionExtractor(toBaseExpr)) =>
        f("CONV", numExpr, fromBaseExpr, toBaseExpr)

      // regexpExpressions.scala
      case RegExpReplace(expressionExtractor(subject),
                         expressionExtractor(regexp),
                         expressionExtractor(rep)) =>
        f("REGEXP_REPLACE", subject, regexp, rep)

      // TODO: case RegExpExtract(expressionExtractor(subject), expressionExtractor(regexp), expressionExtractor(idx)) => ???

      // stringExpressions.scala
      case StringReplace(expressionExtractor(srcExpr),
                         expressionExtractor(searchExpr),
                         expressionExtractor(replaceExpr)) =>
        f("REPLACE", srcExpr, searchExpr, replaceExpr)
      case SubstringIndex(expressionExtractor(strExpr),
                          expressionExtractor(delimExpr),
                          expressionExtractor(countExpr)) =>
        f("SUBSTRING_INDEX", strExpr, delimExpr, countExpr)
      case StringLocate(expressionExtractor(substr),
                        expressionExtractor(str),
                        expressionExtractor(start)) =>
        f("LOCATE", substr, str, start)
      case StringLPad(expressionExtractor(str),
                      expressionExtractor(len),
                      expressionExtractor(pad)) =>
        f("LPAD", str, len, pad)
      case StringRPad(expressionExtractor(str),
                      expressionExtractor(len),
                      expressionExtractor(pad)) =>
        f("RPAD", str, len, pad)
      case Substring(expressionExtractor(str),
                     expressionExtractor(pos),
                     expressionExtractor(len)) =>
        f("SUBSTR", str, pos, len)

      // TODO: case StringTranslate(expressionExtractor(srcExpr), expressionExtractor(matchingExpr), expressionExtractor(replaceExpr)) => ???

      // ----------------------------------
      // Unary Expressions
      // ----------------------------------

      // arithmetic.scala
      case UnaryMinus(expressionExtractor(child))    => f("-", child)
      case UnaryPositive(expressionExtractor(child)) => f("+", child)
      case Abs(expressionExtractor(child))           => f("ABS", child)

      // bitwiseExpressions.scala
      case BitwiseNot(expressionExtractor(expr)) => f("~", expr)

      // Cast.scala
      case Cast(expressionExtractor(child), dataType, _) =>
        dataType match {
          case TimestampType => cast(child, "DATETIME(6)")
          case DateType      => cast(child, "DATE")

          case dt: DecimalType => makeDecimal(child, dt.precision, dt.scale)

          case StringType  => cast(child, "CHAR")
          case BinaryType  => cast(child, "BINARY")
          case ShortType   => op("!:>", child, "SMALLINT")
          case IntegerType => op("!:>", child, "INT")
          case LongType    => op("!:>", child, "BIGINT")
          case FloatType   => op("!:>", child, "FLOAT")
          case DoubleType  => op("!:>", child, "DOUBLE")
          case BooleanType => op("!:>", child, "BOOL")

          // MemSQL doesn't know how to handle this cast, pass it through AS is
          case _ => child
        }

      // TODO: case UpCast(expressionExtractor(child), dataType, walkedTypePath) => ???

      // datetimeExpressions.scala
      case Hour(expressionExtractor(child), _)     => f("HOUR", child)
      case Minute(expressionExtractor(child), _)   => f("MINUTE", child)
      case Second(expressionExtractor(child), _)   => f("SECOND", child)
      case DayOfYear(expressionExtractor(child))   => f("DAYOFYEAR", child)
      case Year(expressionExtractor(child))        => f("YEAR", child)
      case Quarter(expressionExtractor(child))     => f("QUARTER", child)
      case Month(expressionExtractor(child))       => f("MONTH", child)
      case DayOfMonth(expressionExtractor(child))  => f("DAY", child)
      case DayOfWeek(expressionExtractor(child))   => f("DAYOFWEEK", child)
      case WeekOfYear(expressionExtractor(child))  => f("WEEK", child, "3")
      case LastDay(expressionExtractor(startDate)) => f("LAST_DAY", startDate)

      case ParseToDate(expressionExtractor(left), None, _) => f("DATE", left)
      case ParseToDate(expressionExtractor(left), Some(expressionExtractor(format)), _) =>
        f("TO_DATE", left, format)

      case ParseToTimestamp(expressionExtractor(left), None, _) => f("TIMESTAMP", left)
      case ParseToTimestamp(expressionExtractor(left), Some(expressionExtractor(format)), _) =>
        f("TO_TIMESTAMP", left, format)

      // decimalExpressions.scala

      // MakeDecimal and UnscaledValue value are used in DecimalAggregates optimizer
      // This optimizer replace Decimals inside of the sum and aggregate expressions to the Longs using UnscaledValue
      // and then casts the result back to Decimal using MakeDecimal
      case MakeDecimal(expressionExtractor(child), p, s) =>
        makeDecimal(op("/", child, math.pow(10.0, s).toString), p, s)

      case UnscaledValue(decimalExpressionExtractor(child, precision, scale)) =>
        op("!:>", op("*", child, math.pow(10.0, scale).toString), "BIGINT")

      // hash.scala
      case Md5(expressionExtractor(child))   => f("MD5", child)
      case Sha1(expressionExtractor(child))  => f("SHA1", child)
      case Crc32(expressionExtractor(child)) => f("CRC32", child)

      // mathExpressions.scala
      case Acos(expressionExtractor(child))      => f("ACOS", child)
      case Asin(expressionExtractor(child))      => f("ASIN", child)
      case Atan(expressionExtractor(child))      => f("ATAN", child)
      case Ceil(expressionExtractor(child))      => f("CEIL", child)
      case Cos(expressionExtractor(child))       => f("COS", child)
      case Exp(expressionExtractor(child))       => f("EXP", child)
      case Expm1(expressionExtractor(child))     => block(func("EXP", child) + "- 1")
      case Floor(expressionExtractor(child))     => f("FLOOR", child)
      case Log(expressionExtractor(child))       => f("LOG", child)
      case Log2(expressionExtractor(child))      => f("LOG2", child)
      case Log10(expressionExtractor(child))     => f("LOG10", child)
      case Log1p(expressionExtractor(child))     => f("LOG", child + "+ 1")
      case Signum(expressionExtractor(child))    => f("SIGN", child)
      case Sin(expressionExtractor(child))       => f("SIN", child)
      case Sqrt(expressionExtractor(child))      => f("SQRT", child)
      case Tan(expressionExtractor(child))       => f("TAN", child)
      case Cot(expressionExtractor(child))       => f("COT", child)
      case ToDegrees(expressionExtractor(child)) => f("DEGREES", child)
      case ToRadians(expressionExtractor(child)) => f("RADIANS", child)
      case Bin(expressionExtractor(child))       => f("BIN", child)
      case Hex(expressionExtractor(child))       => f("HEX", child)
      case Unhex(expressionExtractor(child))     => f("UNHEX", child)

      // tanh(x) = (exp(x) - exp(-x)) / (exp(x) + exp(-x))
      case Tanh(expressionExtractor(child)) =>
        op("/",
           op("-", f("EXP", child), f("EXP", f("-", child))),
           op("+", f("EXP", child), f("EXP", f("-", child))))

      // sinh(x) = (exp(x) - exp(-x)) / 2
      case Sinh(expressionExtractor(child)) =>
        op("/", op("-", f("EXP", child), f("EXP", f("-", child))), "2")

      // cosh(x) = (exp(x) + exp(-x)) / 2
      case Cosh(expressionExtractor(child)) =>
        op("/", op("+", f("EXP", child), f("EXP", f("-", child))), "2")

      case Rint(expressionExtractor(child)) => f("ROUND", child, "0")

      // TODO: case Factorial(expressionExtractor(child)) => ???
      // TODO: case Cbrt(expressionExtractor(child))      => f("POW", child, op("/", "1", "3"))
      //  We need to wait for the engine to implement precise cbrt

      // nullExpressions.scala
      case IfNull(expressionExtractor(left), expressionExtractor(right), _) =>
        f("COALESCE", left, right)
      case NullIf(expressionExtractor(left), expressionExtractor(right), _) =>
        f("NULLIF", left, right)
      case Nvl(expressionExtractor(left), expressionExtractor(right), _) =>
        f("COALESCE", left, right)
      case IsNull(expressionExtractor(child))    => block(child) + "IS NULL"
      case IsNotNull(expressionExtractor(child)) => block(child) + "IS NOT NULL"

      case Nvl2(expressionExtractor(expr1),
                expressionExtractor(expr2),
                expressionExtractor(expr3),
                _) =>
        f("IF", expr1 + "IS NOT NULL", expr2, expr3)

      // predicates.scala
      case Not(expressionExtractor(child)) => block(Raw("NOT") + child)

      // randomExpressions.scala
      case Rand(expressionExtractor(child)) => f("RAND", child)
      // TODO: case Randn(expressionExtractor(child)) => ???

      // SortOrder.scala
      // in MemSQL, nulls always come first when direction = ascending
      case SortOrder(expressionExtractor(child), Ascending, NullsFirst, _) => block(child) + "ASC"
      // in MemSQL, nulls always come last when direction = descending
      case SortOrder(expressionExtractor(child), Descending, NullsLast, _) => block(child) + "DESC"

      // stringExpressions.scala
      case Upper(expressionExtractor(child)) => f("UPPER", child)
      case Lower(expressionExtractor(child)) => f("LOWER", child)

      case StringSpace(expressionExtractor(child)) => f("LPAD", "", child, StringVar(" "))

      case Right(expressionExtractor(str), expressionExtractor(len), _) => f("RIGHT", str, len)
      case Left(expressionExtractor(str), expressionExtractor(len), _)  => f("LEFT", str, len)
      case Length(expressionExtractor(child))                           => f("CHAR_LENGTH", child)
      case BitLength(expressionExtractor(child))                        => block(func("LENGTH", child) + "* 8")
      case OctetLength(expressionExtractor(child))                      => f("LENGTH", child)
      case Ascii(expressionExtractor(child))                            => f("ASCII", child)
      case Chr(expressionExtractor(child))                              => f("CHAR", child)
      case Base64(expressionExtractor(child))                           => f("TO_BASE64", child)
      case UnBase64(expressionExtractor(child))                         => f("FROM_BASE64", child)

      // TODO: case InitCap(expressionExtractor(child)) => ???
      // TODO: case StringReverse(expressionExtractor(child)) => ???
      // TODO: case SoundEx(expressionExtractor(child)) => ???
    }
  }
}
