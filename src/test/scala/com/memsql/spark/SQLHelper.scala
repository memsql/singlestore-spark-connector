package com.memsql.spark

import java.sql.SQLException

import scala.annotation.tailrec

object SQLHelper {

  @tailrec
  def isSQLExceptionWithCode(e: Throwable, codes: List[Integer]): Boolean = e match {
    case e: SQLException if codes.contains(e.getErrorCode) => true
    case e if e.getCause != null                           => isSQLExceptionWithCode(e.getCause, codes)
    case e =>
      e.printStackTrace()
      false
  }
}
