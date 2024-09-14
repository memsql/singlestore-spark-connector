package com.singlestore.spark

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.concurrent.{Executors, ForkJoinPool}
import com.singlestore.spark.SQLGen.VariableList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{DataSourceTelemetry, InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}

case class SinglestoreRDD(query: String,
                          variables: VariableList,
                          options: SinglestoreOptions,
                          schema: StructType,
                          expectedOutput: Seq[Attribute],
                          resultMustBeSorted: Boolean,
                          parallelReadRepartitionColumns: Seq[String],
                          @transient val sc: SparkContext,
                          randHex: String,
                          telemetryMetrics: DataSourceTelemetry)
    extends RDD[Row](sc, Nil) {
  val (parallelReadType, partitions_) = SinglestorePartitioner(this).getPartitions

  // Spark serializes RDD object and sends it to executor
  // On executor sc value will be null as it is marked as transient
  def isRunOnExecutor: Boolean = sc == null

  val applicationId: String = sc.applicationId

  val aggregatorParallelReadUsed: Boolean =
    parallelReadType.contains(ReadFromAggregators) ||
      parallelReadType.contains(ReadFromAggregatorsMaterialized)

  if (!isRunOnExecutor && aggregatorParallelReadUsed) {
    AggregatorParallelReadListenerAdder.addRDD(this)
  }

  override def finalize(): Unit = {
    if (!isRunOnExecutor && aggregatorParallelReadUsed) {
      AggregatorParallelReadListenerAdder.deleteRDD(this)
    }
    super.finalize()
  }

  override protected def getPartitions: Array[Partition] = partitions_

  override def compute(rawPartition: Partition, context: TaskContext): Iterator[Row] = {
    val multiPartition: SinglestoreMultiPartition =
      rawPartition.asInstanceOf[SinglestoreMultiPartition]
    val threadPool = new ForkJoinPool(multiPartition.partitions.size)
    try {
      val executionContext =
        ExecutionContext.fromExecutor(threadPool)
      val future: Future[Seq[Iterator[Row]]] = Future.sequence(multiPartition.partitions.map(p =>
        Future(computeSinglePartition(p, context))(executionContext)))

      Await.result(future, Duration.Inf).foldLeft(Iterator[Row]())(_ ++ _)
    } finally {
      threadPool.shutdownNow()
    }
  }

  def computeSinglePartition(rawPartition: SinglestorePartition,
                             context: TaskContext): Iterator[Row] = {
    var closed                          = false
    var rs: ResultSet                   = null
    var stmt: PreparedStatement         = null
    var conn: Connection                = null
    val partition: SinglestorePartition = rawPartition.asInstanceOf[SinglestorePartition]

    def tryClose(name: String, what: AutoCloseable): Unit = {
      try {
        if (what != null) { what.close() }
      } catch {
        case e: Exception => logWarning(s"Exception closing $name", e)
      }
    }

    val ErrResultTableNotExistCode = 2318

    def close(): Unit = {
      if (closed) { return }
      tryClose("resultset", rs)
      tryClose("statement", stmt)
      tryClose("connection", conn)
      closed = true
    }

    context.addTaskCompletionListener {
      new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = close()
      }
    }

    telemetryMetrics.setPartitionId(Some(partition.index.toString))

    conn = SinglestoreConnectionPool.getConnection(partition.connectionInfo)
    if (aggregatorParallelReadUsed) {
      val tableName = JdbcHelpers.getResultTableName(applicationId,
                                                     context.stageId(),
                                                     id,
                                                     context.stageAttemptNumber(),
                                                     randHex)

      val finalQuery = JdbcHelpers.appendTagsToQuery(
        options,
        JdbcHelpers.getSelectFromResultTableQuery(tableName, partition.index)
      )
      stmt = conn.prepareStatement(finalQuery)

      val startTime = System.currentTimeMillis()
      val timeout = parallelReadType match {
        case Some(ReadFromAggregators) =>
          options.parallelReadTableCreationTimeoutMS
        case Some(ReadFromAggregatorsMaterialized) =>
          options.parallelReadMaterializedTableCreationTimeoutMS
        case _ =>
          0
      }

      var lastError: java.sql.SQLException = null
      var delay                            = 50
      val maxDelay                         = 10000
      while (rs == null && (timeout == 0 || System.currentTimeMillis() - startTime < timeout)) {
        try {
          context.emitMetricsLog(
            telemetryMetrics.compileGlobalTelemetryTagsMap(Some(finalQuery))
          )

          telemetryMetrics.setQuerySubmissionTime()
          rs = stmt.executeQuery()
        } catch {
          case e: java.sql.SQLException if e.getErrorCode == ErrResultTableNotExistCode =>
            lastError = e
            delay = Math.min(maxDelay, delay * 2)
            Thread.sleep(delay)
        }
      }

      if (rs == null) {
        throw new java.sql.SQLException("Failed to read data from result table", lastError)
      }
    } else {
      val finalQuery = JdbcHelpers.appendTagsToQuery(options, partition.query)
      stmt = conn.prepareStatement(finalQuery)

      context.emitMetricsLog(
        telemetryMetrics.compileGlobalTelemetryTagsMap(Some(finalQuery))
      )

      JdbcHelpers.fillStatement(stmt, partition.variables)

      telemetryMetrics.setQuerySubmissionTime()
      rs = stmt.executeQuery()
    }

    var rowsIter = JdbcUtils.resultSetToRows(rs, schema)

    telemetryMetrics.readColumnCount.set(rs.getMetaData.getColumnCount)

    if (expectedOutput.nonEmpty) {
      val schemaDatatypes   = schema.map(_.dataType)
      val expectedDatatypes = expectedOutput.map(_.dataType)

      def getOrNull(f: => Any, r: Row, i: Int): Any = {
        if (r.isNullAt(i)) null
        else f
      }

      if (schemaDatatypes != expectedDatatypes) {
        val columnEncoders = schemaDatatypes.zip(expectedDatatypes).zipWithIndex.map {
          case ((_: StringType, _: NullType), _) => ((_: Row) => null)
          case ((_: ShortType, _: BooleanType), i) =>
            (r: Row) =>
              getOrNull(r.getShort(i) != 0, r, i)
          case ((_: IntegerType, _: BooleanType), i) =>
            (r: Row) =>
              getOrNull(r.getInt(i) != 0, r, i)
          case ((_: LongType, _: BooleanType), i) =>
            (r: Row) =>
              getOrNull(r.getLong(i) != 0, r, i)

          case ((_: ShortType, _: ByteType), i) =>
            (r: Row) =>
              getOrNull(r.getShort(i).toByte, r, i)
          case ((_: IntegerType, _: ByteType), i) =>
            (r: Row) =>
              getOrNull(r.getInt(i).toByte, r, i)
          case ((_: LongType, _: ByteType), i) =>
            (r: Row) =>
              getOrNull(r.getLong(i).toByte, r, i)

          case ((l, r), i) =>
            options.assert(l == r, s"SinglestoreRDD: unable to encode ${l} into ${r}")
            ((r: Row) => getOrNull(r.get(i), r, i))
        }

        rowsIter = rowsIter
          .map(row => Row.fromSeq(columnEncoders.map(_(row))))
      }
    }

    CompletionIterator[Row, Iterator[Row]](new InterruptibleIterator[Row](context, rowsIter, telemetryMetrics), close)
  }

}
