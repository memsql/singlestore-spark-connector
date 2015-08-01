package com.memsql.superapp

import org.scalatest._
import scala.sys.process._
import com.memsql.superapp.util.Paths

abstract class UnitSpec extends FlatSpec with Matchers with OptionValues
  with Inside with Inspectors with BeforeAndAfter with BeforeAndAfterEach
  with BeforeAndAfterAll with OneInstancePerTest {

  override def beforeAll(): Unit = {
    "rm -rf test_root" !!

    Paths.initialize("test_root")
  }
}
