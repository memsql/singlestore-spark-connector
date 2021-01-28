package com.singlestore.spark

import org.slf4j.{Logger, LoggerFactory}

trait LazyLogging {
  @transient
  protected lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)
}
