package com.memsql.spark.interface.util

import scala.collection.mutable

class BoundedMap[A, B](limit: Int) extends mutable.LinkedHashMap[A, B] {
  override def update(key: A, value: B): Unit = {
    while (size >= limit) {
      remove(head._1)
    }
    super.update(key, value)
  }
}
