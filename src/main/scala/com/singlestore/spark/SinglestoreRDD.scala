package com.singlestore.spark

import java.sql.{Connection, PreparedStatement, ResultSet, SQLTransientConnectionException}

import com.singlestore.spark.SQLGen.{SinglestoreVersion, VariableList}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

case class SinglestoreRDD(query: String,
                          variables: VariableList,
                          options: SinglestoreOptions,
                          schema: StructType,
                          expectedOutput: Seq[Attribute],
                          resultMustBeSorted: Boolean,
                          parallelReadRepartitionColumns: Seq[String],
                          @transient val sc: SparkContext)
    extends RDD[Row](sc, Nil) {
  val (parallelReadType, partitions_) = SinglestorePartitioner(this).getPartitions

  // Spark serializes RDD object and sends it to executor
  // On executor sc value will be null as it is marked as transient
  def isRunOnExecutor: Boolean = sc == null

  val applicationId: String = sc.applicationId

  lazy val singlestoreVersion: SinglestoreVersion = options.version

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
    var closed                          = false
    var rs: ResultSet                   = null
    var stmt: PreparedStatement         = null
    var conn: Connection                = null
    var partition: SinglestorePartition = rawPartition.asInstanceOf[SinglestorePartition]

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

    conn = SinglestoreConnectionPool.getConnection(partition.connectionInfo)
    if (aggregatorParallelReadUsed) {
      val tableName = JdbcHelpers.getResultTableName(applicationId, context.stageId(), id)

      stmt =
        conn.prepareStatement(JdbcHelpers.getSelectFromResultTableQuery(tableName, partition.index))

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
      while (rs == null && (timeout == 0 || System.currentTimeMillis() - startTime < timeout)) {
        try {
          rs = stmt.executeQuery()
        } catch {
          case e: java.sql.SQLException if e.getErrorCode == ErrResultTableNotExistCode =>
            lastError = e
            Thread.sleep(10)
        }
      }

      if (rs == null) {
        throw new java.sql.SQLException("Failed to read data from result table", lastError)
      }
    } else {
      stmt = conn.prepareStatement(partition.query)
      JdbcHelpers.fillStatement(stmt, partition.variables)
      rs = stmt.executeQuery()
    }

    var rowsIter = JdbcUtils.resultSetToRows(rs, schema)

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

    CompletionIterator[Row, Iterator[Row]](new InterruptibleIterator[Row](context, rowsIter), close)
  }

}
