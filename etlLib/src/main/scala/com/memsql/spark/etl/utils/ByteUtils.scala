package com.memsql.spark.etl.utils

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object ByteUtils extends Serializable {
  def bytesToUTF8String(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)
  def bytesToLong(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong
  def bytesToShort(bytes: Array[Byte]): Short = ByteBuffer.wrap(bytes).getShort
  def bytesToDouble(bytes: Array[Byte]): Double = ByteBuffer.wrap(bytes).getDouble
  def bytesToFloat(bytes: Array[Byte]): Float = ByteBuffer.wrap(bytes).getFloat
  def bytesToInt(bytes: Array[Byte]): Int = ByteBuffer.wrap(bytes).getInt

  def utf8StringToBytes(x: String): Array[Byte] = x.getBytes(StandardCharsets.UTF_8)
  def longToBytes(x: Long): Array[Byte] = ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE).putLong(x).array
  def shortToBytes(x: Short): Array[Byte] = ByteBuffer.allocate(java.lang.Short.SIZE / java.lang.Byte.SIZE).putShort(x).array
  def doubleToBytes(x: Double): Array[Byte] = ByteBuffer.allocate(java.lang.Double.SIZE / java.lang.Byte.SIZE).putDouble(x).array
  def floatToBytes(x: Float): Array[Byte] = ByteBuffer.allocate(java.lang.Float.SIZE / java.lang.Byte.SIZE).putFloat(x).array
  def intToBytes(x: Int): Array[Byte] = ByteBuffer.allocate(java.lang.Integer.SIZE / java.lang.Byte.SIZE).putInt(x).array
}
