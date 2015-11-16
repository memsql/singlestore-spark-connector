package com.memsql.spark.interface.util

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.memsql.spark.etl.utils.Logging
import com.memsql.spark.interface.Config
import com.fasterxml.jackson.databind.ObjectMapper
import java.sql.{PreparedStatement, Connection, DriverManager, Statement}

import scala.util.control.NonFatal

object Checkpoint extends Logging {
  var config: Config = null
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def initialize(config: Config): Unit = {
    this.config = config

    var conn: Connection = null
    var stmt: Statement = null

    try {
      conn = DriverManager.getConnection(s"jdbc:mysql://${config.dbHost}:${config.dbPort}/information_schema", config.dbUser, config.dbPassword)
      conn.setAutoCommit(true)
      stmt = conn.createStatement
      stmt.execute("SET as_aggregator=false")  // in order to create the checkpoint database only on the master agg
      stmt.execute(s"CREATE DATABASE IF NOT EXISTS `${config.metadatadbName}`")
      stmt.execute(s"USE `${config.metadatadbName}`")
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS checkpoints
        (pipeline_id TEXT PRIMARY KEY, checkpoint_data JSON NOT NULL)
      """)
      stmt.execute("SET as_aggregator=true")
    } finally {
      if (stmt != null && !stmt.isClosed) stmt.close
      if (conn != null && !conn.isClosed) conn.close
    }
  }

  def getCheckpointData(pipelineId: String): Option[Map[String, Any]] = {
    var conn: Connection = null
    var stmt: PreparedStatement = null

    try {
      conn = DriverManager.getConnection(s"jdbc:mysql://${config.dbHost}:${config.dbPort}/${config.metadatadbName}", config.dbUser, config.dbPassword)
      conn.setAutoCommit(true)
      stmt = conn.prepareStatement("SELECT * FROM checkpoints WHERE pipeline_id = ?")
      stmt.setString(1, pipelineId)
      val rs = stmt.executeQuery

      rs.next match {
        case true => {
          val checkpointData = rs.getString("checkpoint_data")
          logDebug(s"Retrieving checkpoint data for pipeline $pipelineId")
          Some(mapper.readValue(checkpointData, classOf[Map[String, Any]]))
        }
        case false => None
      }
    } catch {
      case e: JsonProcessingException => {
        logWarn(s"Checkpoint data for pipeline $pipelineId is invalid JSON, it will be ignored", e)
        None
      }
    } finally {
      if (stmt != null && !stmt.isClosed) stmt.close
      if (conn != null && !conn.isClosed) conn.close
    }
  }

  def setCheckpointData(pipelineId: String, data: Map[String, Any]): Unit = {
    val jsonData = mapper.writeValueAsString(data)

    var conn: Connection = null
    var stmt: PreparedStatement = null
    try {
      conn = DriverManager.getConnection(s"jdbc:mysql://${config.dbHost}:${config.dbPort}/${config.metadatadbName}", config.dbUser, config.dbPassword)
      stmt = conn.prepareStatement("REPLACE INTO checkpoints (pipeline_id, checkpoint_data) VALUES (?, ?)")

      stmt.setString(1, pipelineId)
      stmt.setString(2, jsonData)
      stmt.executeUpdate
    } catch {
      case NonFatal(e) => logWarn(s"Failed to save checkpoint data for pipeline $pipelineId, it will be ignored", e)
    } finally {
      if (stmt != null && !stmt.isClosed) stmt.close
      if (conn != null && !conn.isClosed) conn.close
    }
  }
}
