package com.memsql.spark.interface.util

import java.text.SimpleDateFormat

import org.apache.log4j.{AppenderSkeleton, PatternLayout};
import org.apache.log4j.spi.LoggingEvent;
import scala.collection.mutable.Queue

class ArrayLogAppender extends AppenderSkeleton {

  private var logEntries: Queue[String] = new Queue[String]()
  private val dateFormat = new SimpleDateFormat()
  // This is the same logging layout we use in
  // com.memsql.spark.etl.utils.Logging, but without newlines at the end and
  // without the logger name.
  private val patternLayout = new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p: %m")

  def getLogEntries(): List[String] = logEntries.toList

  override def append(event: LoggingEvent) = {
    logEntries.enqueue(patternLayout.format(event))
  }

  override def close() = {
  }

  override def requiresLayout() = false
}
