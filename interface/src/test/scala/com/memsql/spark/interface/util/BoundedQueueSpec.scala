package com.memsql.spark.interface.util

import com.memsql.spark.interface.UnitSpec

class BoundedQueueSpec extends UnitSpec {
  "BoundedQueue" should "let items be queued and dequeued" in {
    val boundedQueue = new BoundedQueue[String](10)
    boundedQueue.enqueue("test 1", "test 2")
    boundedQueue.enqueue("test 3")
    boundedQueue.enqueue("test 4")

    assert(boundedQueue.size == 4)

    assert(boundedQueue.dequeue() == "test 1")
    assert(boundedQueue.dequeue() == "test 2")
    assert(boundedQueue.dequeue() == "test 3")
    assert(boundedQueue.dequeue() == "test 4")
    intercept[NoSuchElementException] {
      boundedQueue.dequeue()
    }
  }

  it should "enforce a maximum size" in {
    val boundedQueue = new BoundedQueue[String](2)
    boundedQueue.enqueue("test 1", "test 2")
    boundedQueue.enqueue("test 3")
    boundedQueue.enqueue("test 4")

    assert(boundedQueue.size == 2)

    assert(boundedQueue.dequeue() == "test 3")
    assert(boundedQueue.dequeue() == "test 4")
    intercept[NoSuchElementException] {
      boundedQueue.dequeue()
    }
  }
}
