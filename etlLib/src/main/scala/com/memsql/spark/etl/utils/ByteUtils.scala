package com.memsql.spark.etl.utils

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.util.{Failure, Try}
import scala.util.control.NonFatal

class ByteUtilsException(message: String, nestedException: Throwable) extends Exception(message, nestedException)

/**
 * Convenience class for serializing and deserializing byte arrays.
 */
object ByteUtils extends Serializable {
  private def wrapException[T](x: => T, errorMessage: String): T = {
    Try(x).recoverWith {
      case NonFatal(e) => Failure(new ByteUtilsException(errorMessage, e))
    }.get
  }

  /**
   * Deserializes a byte array into a UTF-8 encoded [[scala.Predef.String]].
   * Throws [[ByteUtilsException]] if there was an error during deserialization.
   *
   * @param bytes The byte array to be deserialized>
   * @return The deserialized [[scala.Predef.String]].
   */
  def bytesToUTF8String(bytes: Array[Byte]): String =
    wrapException(new String(bytes, StandardCharsets.UTF_8), "Error deserializing bytes to UTF-8 string")

  /**
   * Deserializes a byte array into a [[scala.Long]].
   * Throws [[ByteUtilsException]] if there was an error during deserialization.
   *
   * @param bytes The byte array to be deserialized>
   * @return The deserialized [[scala.Long]].
   */
  def bytesToLong(bytes: Array[Byte]): Long =
    wrapException(ByteBuffer.wrap(bytes).getLong, "Error deserializing bytes to long")

  /**
   * Deserializes a byte array into a [[scala.Short]].
   * Throws [[ByteUtilsException]] if there was an error during deserialization.
   *
   * @param bytes The byte array to be deserialized>
   * @return The deserialized [[scala.Short]].
   */
  def bytesToShort(bytes: Array[Byte]): Short =
    wrapException(ByteBuffer.wrap(bytes).getShort, "Error deserializing bytes to short")

  /**
   * Deserializes a byte array into a [[scala.Double]]>
   * Throws [[ByteUtilsException]] if there was an error during deserialization.
   *
   * @param bytes The byte array to be deserialized>
   * @return The deserialized [[scala.Double]].
   */
  def bytesToDouble(bytes: Array[Byte]): Double =
    wrapException(ByteBuffer.wrap(bytes).getDouble, "Error deserializing bytes to double")

  /**
   * Deserializes a byte array into a [[scala.Float]].
   * Throws [[ByteUtilsException]] if there was an error during deserialization.
   *
   * @param bytes The byte array to be deserialized>
   * @return The deserialized [[scala.Float]].
   */
  def bytesToFloat(bytes: Array[Byte]): Float =
    wrapException(ByteBuffer.wrap(bytes).getFloat, "Error deserializing bytes to float")

  /**
   * Deserializes a byte array into a [[scala.Int]].
   * Throws [[ByteUtilsException]] if there was an error during deserialization.
   *
   * @param bytes The byte array to be deserialized>
   * @return The deserialized [[scala.Int]].
   */
  def bytesToInt(bytes: Array[Byte]): Int =
    wrapException(ByteBuffer.wrap(bytes).getInt, "Error deserializing bytes to int")


  /**
   * Serializes a UTF-8 encoded [[scala.Predef.String]] into a byte array.
   * @param x The [[scala.Predef.String]] to be serialized.
   * @return The serialized byte array.
   */
  def utf8StringToBytes(x: String): Array[Byte] =
    wrapException(x.getBytes(StandardCharsets.UTF_8), "Error serializing UTF-8 string bytes")

  /**
   * Serializes a [[scala.Long]] into a byte array.
   * @param x The [[scala.Long]] to be serialized.
   * @return The serialized byte array.
   */
  def longToBytes(x: Long): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE).putLong(x).array, "Error serializing long to bytes")

  /**
   * Serializes a [[scala.Short]] into a byte array.
   * @param x The [[scala.Short]] to be serialized.
   * @return The serialized byte array.
   */
  def shortToBytes(x: Short): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Short.SIZE / java.lang.Byte.SIZE).putShort(x).array, "Error serializing short to bytes")

  /**
   * Serializes a [[scala.Double]] into a byte array.
   * @param x The [[scala.Double]] to be serialized.
   * @return The serialized byte array.
   */
  def doubleToBytes(x: Double): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Double.SIZE / java.lang.Byte.SIZE).putDouble(x).array, "Error serializing double to bytes")

  /**
   * Serializes a [[scala.Float]] into a byte array.
   * @param x The [[scala.Float]] to be serialized.
   * @return The serialized byte array.
   */
  def floatToBytes(x: Float): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Float.SIZE / java.lang.Byte.SIZE).putFloat(x).array, "Error serializing float to bytes")

  /**
   * Serializes a [[scala.Int]] into a byte array.
   * @param x The [[scala.Int]] to be serialized.
   * @return The serialized byte array.
   */
  def intToBytes(x: Int): Array[Byte] =
    wrapException(ByteBuffer.allocate(java.lang.Integer.SIZE / java.lang.Byte.SIZE).putInt(x).array, "Error serializing int to bytes")
}
