package com.memsql.spark.etl.utils

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.util.{Failure, Try}
import scala.util.control.NonFatal

class ByteUtilsException(message: String, nestedException: Throwable) extends Exception(message, nestedException)

object ByteUtils extends Serializable {
  private def wrapException[T](x: => T, errorMessage: String): T = {
    Try(x).recoverWith {
      case NonFatal(e) => Failure(new ByteUtilsException(errorMessage, e))
    }.get
  }

  def bytesToUTF8String(bytes: Array[Byte]): String =
    wrapException(new String(bytes, StandardCharsets.UTF_8), "Error deserializing bytes to UTF-8 string")

  def bytesToLong(bytes: Array[Byte]): Long =
    wrapException(ByteBuffer.wrap(bytes).getLong, "Error deserializing bytes to long")

  def bytesToShort(bytes: Array[Byte]): Short =
    wrapException(ByteBuffer.wrap(bytes).getShort, "Error deserializing bytes to short")

  def bytesToDouble(bytes: Array[Byte]): Double =
    wrapException(ByteBuffer.wrap(bytes).getDouble, "Error deserializing bytes to double")

  def bytesToFloat(bytes: Array[Byte]): Float =
    wrapException(ByteBuffer.wrap(bytes).getFloat, "Error deserializing bytes to float")

  def bytesToInt(bytes: Array[Byte]): Int =
    wrapException(ByteBuffer.wrap(bytes).getInt, "Error deserializing bytes to int")


  def utf8StringToBytes(x: String): Array[Byte] =
    wrapException(x.getBytes(StandardCharsets.UTF_8), "Error serializing UTF-8 string bytes")

  def longToBytes(x: Long): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE).putLong(x).array, "Error serializing long to bytes")

  def shortToBytes(x: Short): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Short.SIZE / java.lang.Byte.SIZE).putShort(x).array, "Error serializing short to bytes")

  def doubleToBytes(x: Double): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Double.SIZE / java.lang.Byte.SIZE).putDouble(x).array, "Error serializing double to bytes")

  def floatToBytes(x: Float): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Float.SIZE / java.lang.Byte.SIZE).putFloat(x).array, "Error serializing float to bytes")

  def intToBytes(x: Int): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Integer.SIZE / java.lang.Byte.SIZE).putInt(x).array, "Error serializing int to bytes")
}
