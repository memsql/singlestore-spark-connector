package com.memsql.spark.util

object StringConversionUtils {
  def byteArrayToReadableString(bytes: Array[Byte]): String = {
    // Build up a string with hex encoding such that printable ASCII
    // characters get added as-is but other characters are added as an
    // escape sequence (e.g. \x7f).
    val sb = new StringBuilder()
    bytes.foreach(b => {
      if (b >= 0x20 && b <= 0x7e) {
        sb.append(b.toChar)
      } else {
        sb.append("\\x%02x".format(b))
      }
    })
    sb.toString
  }
}
