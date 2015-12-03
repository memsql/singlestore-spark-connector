package com.memsql.spark

import org.apache.spark.SparkException

class MemSQLException extends Exception

class NoMemSQLNodesAvailableException extends MemSQLException

class SaveToMemSQLException(val exception: SparkException, val successfullyInserted: Long) extends MemSQLException
