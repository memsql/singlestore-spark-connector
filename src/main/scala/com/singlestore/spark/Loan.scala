package com.singlestore.spark

class Loan[A <: AutoCloseable](resource: A) {
  def to[T](handle: A => T): T =
    try handle(resource)
    finally resource.close()
}

object Loan {
  def apply[A <: AutoCloseable](resource: A) = new Loan(resource)
}
