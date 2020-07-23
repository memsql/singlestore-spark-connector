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
      case v: Decimal                                => Raw(v.toString)
      case v: BigDecimal                             => Raw(v.toString)
      case v: Float if java.lang.Float.isFinite(v)   => Raw(v.toString)
      case v: Double if java.lang.Double.isFinite(v) => Raw(v.toString)
    }
  }

  object WindowBoundaryExpression {
    def unapply(arg: Expression): Option[Joinable] = arg match {
      case e: SpecialFrameBoundary       => Some(e.sql)
      case UnaryMinus(Expression(child)) => Some(child + "PRECEDING")
      case Literal(n: Integer, IntegerType) =>
        Some(Raw(Math.abs(n).toString) + (if (n < 0) "PRECEDING" else "FOLLOWING"))
      case Expression(child) => Some(child + "FOLLOWING")
      case _                 => None
    }
  }

  // we need to manually unwrap MonthsBetween since the roundOff argument
  // does not exist in Spark 2.3
  // The roundOff argument truncates the result to 8 digits of precision
  // which we can safely ignore, the user can apply an explicit round if needed
  object MonthsBetweenExpression {
    def unapply(arg: MonthsBetween): Option[(Joinable, Joinable)] =
      for {
        date1 <- Expression.unapply(arg.date1)
        date2 <- Expression.unapply(arg.date2)
      } yield (date1, date2)
  }

  def apply: PartialFunction[Expression, Joinable] = {
    // ----------------------------------
    // Attributes
    // ----------------------------------
    case a: Attribute                       => Attr(a)
    case a @ Alias(Expression(child), name) => alias(child, name, a.exprId)

    // ----------------------------------
    // Literals
    // ----------------------------------
    case GenLiteral(v) => v

    // ----------------------------------
    // Variable Expressions
    // ----------------------------------

    case Coalesce(Expression(Some(child))) => f("COALESCE", child)
    case Least(Expression(Some(child)))    => f("LEAST", child)
    case Greatest(Expression(Some(child))) => f("GREATEST", child)
    case Concat(Expression(Some(child)))   => f("CONCAT", child)
    case Elt(Expression(Some(child)))      => f("ELT", child)

    // ----------------------------------
    // Aggregate Expressions
    // ----------------------------------

    // TODO: AggregateExpression when filter is not None
    // Average.scala
    case AggregateExpression(Average(Expression(child)), _, _, None, _) => f("AVG", child)

    // CentralMomentAgg.scala
    case AggregateExpression(StddevPop(Expression(child)), _, _, None, _) => f("STDDEV_POP", child)
    case AggregateExpression(StddevSamp(Expression(child)), _, _, None, _) =>
      f("STDDEV_SAMP", child)
    case AggregateExpression(VariancePop(Expression(child)), _, _, None, _)  => f("VAR_POP", child)
    case AggregateExpression(VarianceSamp(Expression(child)), _, _, None, _) => f("VAR_SAMP", child)

    // TODO: case Skewness(Expression(child))     => ???
    // TODO: case Kurtosis(Expression(child))     => ???

    // Count.scala
    case AggregateExpression(Count(Expression(None)), _, false, None, _) => Raw("COUNT(*)")

    case AggregateExpression(Count(Expression(Some(children))), _, isDistinct, None, _) =>
      if (isDistinct) {
        Raw("COUNT") + block(Raw("DISTINCT") + children)
      } else {
        f("COUNT", children)
      }

    // Covariance.scala
    // TODO: case CovPopulation(Expression(left), Expression(right)) => ???
    // TODO: case CovSample(Expression(left), Expression(right))     => ???

    // First.scala
    case AggregateExpression(First(Expression(child), Literal(false, BooleanType)),
                             _,
                             _,
                             None,
                             _) =>
      f("ANY_VALUE", child)

    // Last.scala
    case AggregateExpression(Last(Expression(child), Literal(false, BooleanType)), _, _, None, _) =>
      f("ANY_VALUE", child)

    // Max.scala
    case AggregateExpression(Max(Expression(child)), _, _, None, _) => f("MAX", child)

    // Min.scala
    case AggregateExpression(Min(Expression(child)), _, _, None, _) => f("MIN", child)

    // Sum.scala
    case AggregateExpression(Sum(Expression(child)), _, _, None, _) => f("SUM", child)

    // windowExpressions.scala
    case WindowExpression(Expression(child),
                          WindowSpecDefinition(Expression(partitionSpec),
                                               Expression(orderSpec),
                                               Expression(frameSpec))) =>
      child + "OVER" + block(
        partitionSpec.map(Raw("PARTITION BY") + _).getOrElse(empty) +
          orderSpec.map(Raw("ORDER BY") + _).getOrElse(empty) +
          frameSpec
      )

    case UnspecifiedFrame => ""

    case SpecifiedWindowFrame(frameType,
                              WindowBoundaryExpression(lower),
                              WindowBoundaryExpression(upper)) =>
      Raw(frameType.sql) + "BETWEEN" + lower + "AND" + upper

    case Lead(Expression(input), Expression(offset), Literal(null, NullType)) =>
      f("LEAD", input, offset)
    case Lag(Expression(input), Expression(offset), Literal(null, NullType)) =>
      f("LAG", input, offset)
    case RowNumber()              => "ROW_NUMBER()"
    case NTile(Expression(child)) => f("NTILE", child)
    case Rank(_)                  => "RANK()"
    case DenseRank(_)             => "DENSE_RANK()"
    case PercentRank(_)           => "PERCENT_RANK()"

    // TODO: case CumeDist()               => ???

    // ----------------------------------
    // Binary Expressions
    // ----------------------------------

    // arithmetic.scala

    case Add(Expression(left), Expression(right))       => op("+", left, right)
    case Subtract(Expression(left), Expression(right))  => op("-", left, right)
    case Multiply(Expression(left), Expression(right))  => op("*", left, right)
    case Divide(Expression(left), Expression(right))    => op("/", left, right)
    case Remainder(Expression(left), Expression(right)) => op("%", left, right)

    case Pmod(Expression(left), Expression(right)) =>
      block(block(block(left + "%" + right) + "+" + right) + "%" + right)

    // bitwiseExpressions.scala
    case BitwiseAnd(Expression(left), Expression(right)) => op("&", left, right)
    case BitwiseOr(Expression(left), Expression(right))  => op("|", left, right)
    case BitwiseXor(Expression(left), Expression(right)) => op("^", left, right)

    // datetimeExpressions.scala

    // NOTE: we explicitly ignore the timeZoneId field in all of the following expressions
    // The user is required to setup Spark and/or MemSQL with the timezone they want or they
    // will get inconsistent results with/without pushdown.

    case DateAdd(Expression(startDate), Expression(days)) => f("ADDDATE", startDate, days)
    case DateSub(Expression(startDate), Expression(days)) => f("SUBDATE", startDate, days)
    case DateFormatClass(Expression(left), Expression(right), timeZoneId) =>
      f("DATE_FORMAT", left, right)

    case TimeAdd(Expression(start),
                 Literal(v: CalendarInterval, CalendarIntervalType),
                 timeZoneId) => {
      def addMicroseconds(start: Joinable) =
        if (v.microseconds == 0) {
          start
        } else {
          f("DATE_ADD", start, Raw("INTERVAL") + v.microseconds.toString + "MICROSECOND")
        }
      def addDays(start: Joinable) =
        if (v.days == 0) {
          start
        } else {
          f("DATE_ADD", start, Raw("INTERVAL") + v.days.toString + "DAY")
        }
      def addMonths(start: Joinable) =
        if (v.months == 0) {
          start
        } else {
          f("DATE_ADD", start, Raw("INTERVAL") + v.months.toString + "MONTH")
        }
      addMicroseconds(addDays(addMonths(start)))
    }

    case TimeSub(Expression(start),
                 Literal(v: CalendarInterval, CalendarIntervalType),
                 timeZoneId) => {
      def subMicroseconds(start: Joinable) =
        if (v.microseconds == 0) {
          start
        } else {
          f("DATE_SUB", start, Raw("INTERVAL") + v.microseconds.toString + "MICROSECOND")
        }
      def subDays(start: Joinable) =
        if (v.days == 0) {
          start
        } else {
          f("DATE_SUB", start, Raw("INTERVAL") + v.days.toString + "DAY")
        }
      def subMonths(start: Joinable) =
        if (v.months == 0) {
          start
        } else {
          f("DATE_SUB", start, Raw("INTERVAL") + v.months.toString + "MONTH")
        }
      subMicroseconds(subDays(subMonths(start)))
    }

    case FromUTCTimestamp(Expression(timestamp), Expression(timezone)) =>
      f("CONVERT_TZ", timestamp, StringVar("UTC"), timezone)

    case ToUTCTimestamp(Expression(timestamp), Expression(timezone)) =>
      f("CONVERT_TZ", timestamp, timezone, StringVar("UTC"))

    case TruncTimestamp(Expression(format), Expression(timestamp), timeZoneId) => {
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

    case TruncDate(Expression(date), Expression(format)) => {
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

    case MonthsBetweenExpression((date1, date2)) =>
      f("MONTHS_BETWEEN", date1, date2)

    case AddMonths(Expression(startDate), Expression(numMonths)) =>
      f("DATE_ADD", startDate, Raw("INTERVAL") + numMonths + "MONTH")

    // MemSQL and spark support other date formats
    // UnixTime doesn't use format if time is already a dataType or TimestampType
    case ToUnixTimestamp(e @ Expression(timeExp), _, _) if e.dataType == DateType =>
      f("UNIX_TIMESTAMP", timeExp)

    case ToUnixTimestamp(e @ Expression(timeExp), _, _) if e.dataType == TimestampType =>
      f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0")

    case UnixTimestamp(e @ Expression(timeExp), _, _) if e.dataType == DateType =>
      f("UNIX_TIMESTAMP", timeExp)

    case UnixTimestamp(e @ Expression(timeExp), _, _) if e.dataType == TimestampType =>
      f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0")

    case FromUnixTime(Expression(sec), format, timeZoneId)
        if format.foldable && format.dataType == StringType &&
          format.eval().asInstanceOf[UTF8String] == MEMSQL_DEFAULT_TIME_FORMAT =>
      f("FROM_UNIXTIME", sec)

    case NextDay(Expression(startDate), dayOfWeek)
        if dayOfWeek.foldable && dayOfWeek.dataType == StringType =>
      computeNextDay(
        startDate,
        sqlMapValueCaseInsensitive(
          StringVar(dayOfWeek.eval().asInstanceOf[UTF8String].toString),
          DAYS_OF_WEEK_OFFSET_MAP,
          StringVar(null)
        )
      )

    case NextDay(Expression(startDate), Expression(dayOfWeek)) =>
      computeNextDay(startDate,
                     sqlMapValueCaseInsensitive(
                       dayOfWeek,
                       DAYS_OF_WEEK_OFFSET_MAP,
                       StringVar(null)
                     ))

    case DateDiff(Expression(endDate), Expression(startDate)) =>
      f("DATEDIFF", endDate, startDate)

    // hash.scala
    case Sha2(Expression(left), Expression(right)) => f("SHA2", left, right)

    // mathExpressions.scala
    case Atan2(Expression(left), Expression(right))              => f("ATAN2", left, right)
    case Pow(Expression(left), Expression(right))                => f("POWER", left, right)
    case ShiftLeft(Expression(left), Expression(right))          => op("<<", left, right)
    case ShiftRight(Expression(left), Expression(right))         => op(">>", left, right)
    case ShiftRightUnsigned(Expression(left), Expression(right)) => op(">>", left, right)
    case Logarithm(Expression(left), Expression(right))          => f("LOG", left, right)
    case Round(Expression(child), Expression(scale))             => f("ROUND", child, scale)

    case Hypot(Expression(left), Expression(right)) =>
      f("SQRT", op("+", f("POW", left, "2"), f("POW", right, "2")))

    // TODO: case _: BRound => None

    // predicates.scala
    case And(Expression(left), Expression(right))                => op("AND", left, right)
    case Or(Expression(left), Expression(right))                 => op("OR", left, right)
    case EqualTo(Expression(left), Expression(right))            => op("=", left, right)
    case EqualNullSafe(Expression(left), Expression(right))      => op("<=>", left, right)
    case LessThan(Expression(left), Expression(right))           => op("<", left, right)
    case LessThanOrEqual(Expression(left), Expression(right))    => op("<=", left, right)
    case GreaterThan(Expression(left), Expression(right))        => op(">", left, right)
    case GreaterThanOrEqual(Expression(left), Expression(right)) => op(">=", left, right)

    case In(Expression(child), Expression(Some(elements))) =>
      op("IN", child, block(elements))

    case InSet(Expression(child), GenLiteral(elements)) =>
      op(
        "IN",
        child,
        block(elements)
      )

    // regexpExpressions.scala
    case Like(Expression(left), Expression(right), escapeChar: Char) =>
      if (escapeChar == '\\') {
        op("LIKE", left, right)
      } else {
        op("LIKE", left, f("REPLACE", right, escapeChar.toString(), "\\"))
      }
    case RLike(Expression(left), Expression(right)) => op("RLIKE", left, right)

    // stringExpressions.scala
    case Contains(Expression(left), Expression(right)) =>
      op(">", f("INSTR", left, right), "0")
    case StartsWith(Expression(left), Expression(right)) =>
      op("LIKE", left, f("CONCAT", right, StringVar("%")))
    case EndsWith(Expression(left), Expression(right)) =>
      op("LIKE", left, f("CONCAT", StringVar("%"), right))
    case StringInstr(Expression(str), Expression(substr)) => f("INSTR", str, substr)
    case FormatNumber(Expression(x), Expression(d))       => f("FORMAT", x, d)
    case StringRepeat(Expression(child), Expression(times)) =>
      f("LPAD", StringVar(""), times + "*" + f("CHAR_LENGTH", child), child)

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
    case Conv(Expression(numExpr), Expression(fromBaseExpr), Expression(toBaseExpr)) =>
      f("CONV", numExpr, fromBaseExpr, toBaseExpr)

    // regexpExpressions.scala
    case RegExpReplace(Expression(subject), Expression(regexp), Expression(rep)) =>
      f("REGEXP_REPLACE", subject, regexp, rep)

    // TODO: case RegExpExtract(Expression(subject), Expression(regexp), Expression(idx)) => ???

    // stringExpressions.scala
    case StringReplace(Expression(srcExpr), Expression(searchExpr), Expression(replaceExpr)) =>
      f("REPLACE", srcExpr, searchExpr, replaceExpr)
    case SubstringIndex(Expression(strExpr), Expression(delimExpr), Expression(countExpr)) =>
      f("SUBSTRING_INDEX", strExpr, delimExpr, countExpr)
    case StringLocate(Expression(substr), Expression(str), Expression(start)) =>
      f("LOCATE", substr, str, start)
    case StringLPad(Expression(str), Expression(len), Expression(pad)) => f("LPAD", str, len, pad)
    case StringRPad(Expression(str), Expression(len), Expression(pad)) => f("RPAD", str, len, pad)
    case Substring(Expression(str), Expression(pos), Expression(len))  => f("SUBSTR", str, pos, len)

    // TODO: case StringTranslate(Expression(srcExpr), Expression(matchingExpr), Expression(replaceExpr)) => ???

    // ----------------------------------
    // Unary Expressions
    // ----------------------------------

    // arithmetic.scala
    case UnaryMinus(Expression(child))    => f("-", child)
    case UnaryPositive(Expression(child)) => f("+", child)
    case Abs(Expression(child))           => f("ABS", child)

    // bitwiseExpressions.scala
    case BitwiseNot(Expression(expr)) => f("~", expr)

    // Cast.scala
    case Cast(Expression(child), dataType, _) =>
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

    // TODO: case UpCast(Expression(child), dataType, walkedTypePath) => ???

    // datetimeExpressions.scala
    case Hour(Expression(child), _)     => f("HOUR", child)
    case Minute(Expression(child), _)   => f("MINUTE", child)
    case Second(Expression(child), _)   => f("SECOND", child)
    case DayOfYear(Expression(child))   => f("DAYOFYEAR", child)
    case Year(Expression(child))        => f("YEAR", child)
    case Quarter(Expression(child))     => f("QUARTER", child)
    case Month(Expression(child))       => f("MONTH", child)
    case DayOfMonth(Expression(child))  => f("DAY", child)
    case DayOfWeek(Expression(child))   => f("DAYOFWEEK", child)
    case WeekOfYear(Expression(child))  => f("WEEK", child, "3")
    case LastDay(Expression(startDate)) => f("LAST_DAY", startDate)

    case ParseToDate(Expression(left), None, _)                     => f("DATE", left)
    case ParseToDate(Expression(left), Some(Expression(format)), _) => f("TO_DATE", left, format)

    case ParseToTimestamp(Expression(left), None, _) => f("TIMESTAMP", left)
    case ParseToTimestamp(Expression(left), Some(Expression(format)), _) =>
      f("TO_TIMESTAMP", left, format)

    // decimalExpressions.scala
    case MakeDecimal(Expression(child), p: Int, s: Int, false) => makeDecimal(child, p, s)

    // hash.scala
    case Md5(Expression(child))   => f("MD5", child)
    case Sha1(Expression(child))  => f("SHA1", child)
    case Crc32(Expression(child)) => f("CRC32", child)

    // mathExpressions.scala
    case Acos(Expression(child))      => f("ACOS", child)
    case Asin(Expression(child))      => f("ASIN", child)
    case Atan(Expression(child))      => f("ATAN", child)
    case Ceil(Expression(child))      => f("CEIL", child)
    case Cos(Expression(child))       => f("COS", child)
    case Exp(Expression(child))       => f("EXP", child)
    case Expm1(Expression(child))     => block(func("EXP", child) + "- 1")
    case Floor(Expression(child))     => f("FLOOR", child)
    case Log(Expression(child))       => f("LOG", child)
    case Log2(Expression(child))      => f("LOG2", child)
    case Log10(Expression(child))     => f("LOG10", child)
    case Log1p(Expression(child))     => f("LOG", child + "+ 1")
    case Signum(Expression(child))    => f("SIGN", child)
    case Sin(Expression(child))       => f("SIN", child)
    case Sqrt(Expression(child))      => f("SQRT", child)
    case Tan(Expression(child))       => f("TAN", child)
    case Cot(Expression(child))       => f("COT", child)
    case ToDegrees(Expression(child)) => f("DEGREES", child)
    case ToRadians(Expression(child)) => f("RADIANS", child)
    case Bin(Expression(child))       => f("BIN", child)
    case Hex(Expression(child))       => f("HEX", child)
    case Unhex(Expression(child))     => f("UNHEX", child)

    // tanh(x) = (exp(x) - exp(-x)) / (exp(x) + exp(-x))
    case Tanh(Expression(child)) =>
      op("/",
         op("-", f("EXP", child), f("EXP", f("-", child))),
         op("+", f("EXP", child), f("EXP", f("-", child))))

    // sinh(x) = (exp(x) - exp(-x)) / 2
    case Sinh(Expression(child)) =>
      op("/", op("-", f("EXP", child), f("EXP", f("-", child))), "2")

    // cosh(x) = (exp(x) + exp(-x)) / 2
    case Cosh(Expression(child)) =>
      op("/", op("+", f("EXP", child), f("EXP", f("-", child))), "2")

    case Rint(Expression(child)) => f("ROUND", child, "0")

    // TODO: case Factorial(Expression(child)) => ???
    // TODO: case Cbrt(Expression(child))      => f("POW", child, op("/", "1", "3"))
    //  We need to wait for the engine to implement precise cbrt

    // nullExpressions.scala
    case IfNull(Expression(left), Expression(right), _) => f("COALESCE", left, right)
    case NullIf(Expression(left), Expression(right), _) => f("NULLIF", left, right)
    case Nvl(Expression(left), Expression(right), _)    => f("COALESCE", left, right)
    case IsNull(Expression(child))                      => block(child) + "IS NULL"
    case IsNotNull(Expression(child))                   => block(child) + "IS NOT NULL"

    case Nvl2(Expression(expr1), Expression(expr2), Expression(expr3), _) =>
      f("IF", expr1 + "IS NOT NULL", expr2, expr3)

    // predicates.scala
    case Not(Expression(child)) => block(Raw("NOT") + child)

    // randomExpressions.scala
    case Rand(Expression(child)) => f("RAND", child)
    // TODO: case Randn(Expression(child)) => ???

    // SortOrder.scala
    // in MemSQL, nulls always come first when direction = ascending
    case SortOrder(Expression(child), Ascending, NullsFirst, _) => block(child) + "ASC"
    // in MemSQL, nulls always come last when direction = descending
    case SortOrder(Expression(child), Descending, NullsLast, _) => block(child) + "DESC"

    // stringExpressions.scala
    case Upper(Expression(child)) => f("UPPER", child)
    case Lower(Expression(child)) => f("LOWER", child)

    case StringSpace(Expression(child)) => f("LPAD", "", child, StringVar(" "))

    case Right(Expression(str), Expression(len), _) => f("RIGHT", str, len)
    case Left(Expression(str), Expression(len), _)  => f("LEFT", str, len)
    case Length(Expression(child))                  => f("CHAR_LENGTH", child)
    case BitLength(Expression(child))               => block(func("LENGTH", child) + "* 8")
    case OctetLength(Expression(child))             => f("LENGTH", child)
    case Ascii(Expression(child))                   => f("ASCII", child)
    case Chr(Expression(child))                     => f("CHAR", child)
    case Base64(Expression(child))                  => f("TO_BASE64", child)
    case UnBase64(Expression(child))                => f("FROM_BASE64", child)

    // TODO: case InitCap(Expression(child)) => ???
    // TODO: case StringReverse(Expression(child)) => ???
    // TODO: case SoundEx(Expression(child)) => ???
  }
}
