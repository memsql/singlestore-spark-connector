package com.memsql.spark.etl.utils

import org.scalatest._

class ByteUtilsSpec extends FlatSpec {
  import ByteUtils._
  "ByteUtils" should "round trip common types to and from bytes" in {
    assert(bytesToShort(shortToBytes(Short.MinValue)) == Short.MinValue)
    assert(bytesToInt(intToBytes(0)) == 0)
    assert(bytesToLong(longToBytes(Long.MaxValue)) == Long.MaxValue)
    assert(bytesToFloat(floatToBytes(1234.5f)) == 1234.5f)
    assert(bytesToDouble(doubleToBytes(1234.567890f)) == 1234.567890f)
  }

  it should "raise useful exceptions" in {
    intercept[ByteUtilsException] {
      bytesToInt(utf8StringToBytes("0"))
    }

    intercept[ByteUtilsException] {
      bytesToShort(utf8StringToBytes("0"))
    }

    intercept[ByteUtilsException] {
      utf8StringToBytes(null)
    }

    intercept[ByteUtilsException] {
      bytesToDouble(null)
    }
  }
}
