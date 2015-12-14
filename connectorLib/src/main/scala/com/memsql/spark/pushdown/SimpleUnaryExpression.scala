package com.memsql.spark.pushdown

import org.apache.spark.sql.catalyst.expressions._

import scala.collection.immutable.HashSet

object SimpleUnaryExpression {
  /**
    * The following expressions all can be dynamically turned
    * into to MemSQL expressions
    */

  /**
    * For most expressions in SparkSQL, their prettyName is the name of the MemSQL builtin.
    */
  def unapply(expr: UnaryExpression): Option[(String, Expression)] = expr match {
    case
      // Ascii(_) |
      // Crc32(_) |
      // StringReverse(_) |
      // StringSpace(_) |
      Abs(_) |
      Average(_) |
      Bin(_) |
      Count(_) |
      DayOfMonth(_) |
      DayOfYear(_) |
      Hex(_) |
      Hour(_) |
      Minute(_) |
      Second(_) |
      LastDay(_) |
      Lower(_) |
      Max(_) |
      Md5(_) |
      Min(_) |
      Month(_) |
      Quarter(_) |
      Sha1(_) |
      StringTrim(_) |
      StringTrimLeft(_) |
      StringTrimRight(_) |
      Sum(_) |
      Unhex(_) |
      Upper(_) |
      WeekOfYear(_) |
      Year(_) |

      // UnaryMathExpression
      // Exp(_) |
      Cos(_) |
      Tan(_) |
      // Tanh(_) |
      // Cosh(_) |
      Atan(_) |
      Floor(_) |
      Sin(_) |
      Log(_) |
      // Rint(_) |
      Asin(_) |
      Sqrt(_) |
      Ceil(_) |
      // Sinh(_) |
      // Expm1(_) |
      Acos(_) => Some(expr.prettyName.toUpperCase, expr.child)
    case _ => None
  }
}
