package com.memsql.spark

// Copied from spark's CompletionIterator which is private even though it is generically useful

abstract class CompletionIterator[+A, +I <: Iterator[A]](sub: I) extends Iterator[A] {
  private[this] var completed = false
  def next(): A               = sub.next()
  def hasNext: Boolean = {
    val r = sub.hasNext
    if (!r && !completed) {
      completed = true
      completion()
    }
    r
  }

  def completion(): Unit
}

private[spark] object CompletionIterator {
  def apply[A, I <: Iterator[A]](sub: I, completionFunction: => Unit): CompletionIterator[A, I] = {
    new CompletionIterator[A, I](sub) {
      def completion(): Unit = completionFunction
    }
  }
}
