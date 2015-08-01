package com.memsql.superapp

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{WordSpecLike, BeforeAndAfterAll, MustMatchers}

abstract class TestKitSpec(name: String)
  extends TestKit(ActorSystem(name))
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfterAll
  with ImplicitSender {

  override def afterAll() {
    system.shutdown()
  }
}