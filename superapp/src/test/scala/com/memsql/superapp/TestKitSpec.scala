package com.memsql.superapp

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.memsql.superapp.util.Paths
import org.scalatest.concurrent.{ScalaFutures, AsyncAssertions}
import org.scalatest.{WordSpecLike, BeforeAndAfterAll, Matchers}
import org.scalamock.scalatest.MockFactory
import scala.sys.process._

abstract class TestKitSpec(name: String)
  extends TestKit(ActorSystem(name))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ImplicitSender
  with ScalaFutures
  with MockFactory {

  override def beforeAll() {
    "rm -rf test_root" !!

    Paths.initialize("test_root")
  }

  override def afterAll() {
    system.shutdown()
  }
}