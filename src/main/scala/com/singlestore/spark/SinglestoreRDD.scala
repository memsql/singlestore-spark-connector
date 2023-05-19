package com.singlestore.spark

import java.sql.{Connection, PreparedStatement, ResultSet, SQLTransientConnectionException}
import java.util.concurrent.Executors

import com.singlestore.spark.SQLGen.VariableList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import org.apache.log4j.LogManager
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.NextIterator
import org.apache.log4j.Level
import java.sql.Statement

case class SinglestoreRDD(query: String,
                          variables: VariableList,
                          options: SinglestoreOptions,
                          schema: StructType,
                          expectedOutput: Seq[Attribute],
                          resultMustBeSorted: Boolean,
                          parallelReadRepartitionColumns: Seq[String],
                          @transient val sc: SparkContext)
    extends RDD[Row](sc, Nil) {

  class LoggableIterator[+T](val delegate: Iterator[T], index: Integer) extends Iterator[T] {

  def hasNext: Boolean = {
    // log.info(s"SINGLESTORE SPARK $index: Checking if result has more rows")
    val res = delegate.hasNext
    // log.info(s"SINGLESTORE SPARK $index: Checked that result has more rows ($res)")
    res
  }

  def next(): T = {
    // log.info(s"SINGLESTORE SPARK $index: Getting row")
    val res = delegate.next()
    // log.info(s"SINGLESTORE SPARK $index: Got row")
    res
  }
}

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
    LogManager.getLogger("com.singlestore.spark.SinglestoreRDD").setLevel(Level.INFO)
    LogManager.getLogger("com.singlestore.jdbc.client.result.CompleteResult").setLevel(Level.INFO)
    LogManager.getLogger("com.singlestore.jdbc.client.result.UpdatableResult").setLevel(Level.INFO)
    LogManager.getLogger("com.singlestore.jdbc.client.result.StreamingResult").setLevel(Level.INFO)
    LogManager.getLogger("com.singlestore.jdbc.message.client.ClientMessage").setLevel(Level.INFO)
    LogManager.getLogger("com.singlestore.jdbc.client.socket.PacketReader").setLevel(Level.INFO)
    LogManager.getLogger("com.singlestore.jdbc.client.socket.ReadAheadBufferedStream").setLevel(Level.INFO)

    val multiPartition: SinglestoreMultiPartition =
      rawPartition.asInstanceOf[SinglestoreMultiPartition]
    val threadPool = Executors.newFixedThreadPool(multiPartition.partitions.size)
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
    val index = partition.index
    // log.info(s"SINGLESTORE SPARK $index: Reading from partition")

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
      //log.info("PROFILE: " + rs2.getString(1))
      //log.info(s"SINGLESTORE SPARK $index: closing connection")
      tryClose("resultset", rs)
      tryClose("statement", stmt)
      tryClose("connection", conn)
      //log.info(s"SINGLESTORE SPARK $index: closed connection")    
      closed = true
    }

    context.addTaskCompletionListener {
      new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = close()
      }
    }

    conn = SinglestoreConnectionPool.getConnection(partition.connectionInfo)
    if (aggregatorParallelReadUsed) {
      val tableName = JdbcHelpers.getResultTableName(applicationId,
                                                     context.stageId(),
                                                     id,
                                                     context.attemptNumber())

      //log.info(s"SINGLESTORE SPARK $index: Preparing query ${JdbcHelpers.getSelectFromResultTableQuery(tableName, partition.index)}")
      stmt =
        conn.prepareStatement(JdbcHelpers.getSelectFromResultTableQuery(tableName, partition.index))
      stmt.setFetchSize(25000);
      //log.info(s"SINGLESTORE SPARK $index: Preparied query ${JdbcHelpers.getSelectFromResultTableQuery(tableName, partition.index)}")

      val startTime = System.currentTimeMillis()
      val timeout = parallelReadType match {
        case Some(ReadFromAggregators) =>
          options.parallelReadTableCreationTimeoutMS
        case Some(ReadFromAggregatorsMaterialized) =>
          options.parallelReadMaterializedTableCreationTimeoutMS
        case _ =>
          0
      }

 
      //log.info(s"SINGLESTORE SPARK $index: Executing query ${JdbcHelpers.getSelectFromResultTableQuery(tableName, partition.index)}")
      var lastError: java.sql.SQLException = null
      while (rs == null && (timeout == 0 || System.currentTimeMillis() - startTime < timeout)) {
        try {
          rs = stmt.executeQuery()
        } catch {
          case e: java.sql.SQLException if e.getErrorCode == ErrResultTableNotExistCode =>
            lastError = e
            Thread.sleep(10)
        }
      }
      //log.info(s"SINGLESTORE SPARK $index: Executed query ${JdbcHelpers.getSelectFromResultTableQuery(tableName, partition.index)}, result set: ${rs}")

      if (rs == null) {
        throw new java.sql.SQLException("Failed to read data from result table", lastError)
      }
    } else {
      //log.info(s"SINGLESTORE SPARK $index: Preparing query ${partition.query}")
      stmt = conn.prepareStatement(partition.query)
      //log.info(s"SINGLESTORE SPARK $index: Prepared query ${partition.query}")
      JdbcHelpers.fillStatement(stmt, partition.variables)
      //log.info(s"SINGLESTORE SPARK $index: Executing query ${partition.query}")
      rs = stmt.executeQuery()
      //log.info(s"SINGLESTORE SPARK $index: Executed query ${partition.query}, result set: ${rs}")
    }


    //log.info(s"SINGLESTORE SPARK $index: converting result set to rows")
    var rowsIter = JdbcUtils.resultSetToRows(rs, schema)
    //log.info(s"SINGLESTORE SPARK $index: converted result set to rows")    
    rowsIter = new LoggableIterator[Row](rowsIter, index)

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
          .map(row => {
            //log.info(s"SINGLESTORE SPARK $index: mapping row to correct types")    
            val res = Row.fromSeq(columnEncoders.map(_(row)))
            //log.info(s"SINGLESTORE SPARK $index: mapped row to correct types")    
            res
          })
      }
    }

    CompletionIterator[Row, Iterator[Row]](new InterruptibleIterator[Row](context, rowsIter), close)
  }

}
