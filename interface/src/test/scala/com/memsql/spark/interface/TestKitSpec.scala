package com.memsql.spark.interface

import akka.actor.{Actor, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.memsql.spark.interface.api.{ApiService, SparkProgress}
import com.memsql.spark.interface.util.{Clock, Paths}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.sys.process._

case class TestApiActor(mockTime: Clock, sparkProgress: SparkProgress) extends Actor with ApiService {
  override def clock: Clock = mockTime
  override def receive: Receive = handleMessage
}

abstract class TestKitSpec(name: String)
  extends TestKit(ActorSystem(name))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ImplicitSender
  with ScalaFutures {

  override protected def beforeAll() {
    "rm -rf test_root" !!

    Paths.initialize("test_root")

    Class.forName("com.mysql.jdbc.Driver")
  }

  override protected def afterAll() {
    system.shutdown()
  }

  class MockTime extends Clock {
    private var time = 0

    override def currentTimeMillis: Long = time

    def tick(): Unit = {
      time += 1
    }
  }

  class TestException(message: String) extends Exception {
    override def toString: String = s"TestException: $message"
  }
}
