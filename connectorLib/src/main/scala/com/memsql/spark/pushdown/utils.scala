package com.memsql.spark.pushdown

class MemSQLPushdownException(message: String) extends Exception(message)

case class QueryAlias(prefix: String, height: Int=0) {
  override def toString: String = s"${prefix}_$height"

  /**
   * Returns the next QueryAlias in the current tree.
   */
  def next: QueryAlias = this.copy(height=height + 1)

  /**
   * Forks this QueryAlias into two aliases, suitable
   * for using in two parallel Query trees.
   */
  def fork: (QueryAlias, QueryAlias) = (
    QueryAlias(prefix=s"${prefix}_$height"),
    QueryAlias(prefix=s"${prefix}_${height + 1}")
  )
}


/**
 * A OptionSeq wraps a Seq with an Option based on whether or not the Seq is empty.
 */
object OptionSeq {
  def apply(seq: Seq[Any]) =
    if (seq.isEmpty) {
      None
    } else {
      Some(seq)
    }
}

object StringBuilderImplicits {
  implicit class IndentingStringBuilder(val sb: StringBuilder) {
    def indent(depth: Int): StringBuilder = sb.append("  " * depth)
  }
}
