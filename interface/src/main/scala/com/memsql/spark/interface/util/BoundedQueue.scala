package com.memsql.spark.interface.util

import scala.collection.mutable.Queue

class BoundedQueue[A](limit: Int) extends Queue[A] {
  override def enqueue(elems: A*): Unit = {
    var totalSize = size + elems.size
    try {
      while (totalSize > limit) {
        dequeue()
        totalSize -= 1
      }
    } catch {
      case e: NoSuchElementException =>
    }
    super.enqueue(elems: _*)
  }
}
