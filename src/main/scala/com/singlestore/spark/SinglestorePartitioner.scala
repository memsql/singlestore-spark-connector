package com.singlestore.spark

import java.sql.SQLException
import java.util.Properties

import com.singlestore.spark.JdbcHelpers.{getDDLConnProperties, getDMLConnProperties}
import com.singlestore.spark.SQLGen.{SinglestoreVersion, VariableList}
import org.apache.spark.scheduler.MaxNumConcurrentTasks
import org.apache.spark.Partition
import spray.json.DeserializationException
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.Try

// SinglestoreMultiPartition represents single Spark partition reading from >= 1 SingleStore partitions
case class SinglestoreMultiPartition(override val index: Int, partitions: Seq[SinglestorePartition])
    extends Partition

case class SinglestorePartition(
    override val index: Int,
    query: String,
    variables: VariableList,
    connectionInfo: Properties,
) extends Partition

case class SinglestorePartitioner(rdd: SinglestoreRDD) extends LazyLogging {
  private val options: SinglestoreOptions = rdd.options

  private var errorMessages: Map[ParallelReadType, String] = Map.empty

  private val executorWhitelist: Set[String] = Set(
    "Project",
    "Gather",
    "Filter",
    "TableScan",
    "ColumnStoreScan",
    "ColumnStoreFilter",
    "OrderedColumnStoreScan",
    "IndexRangeScan",
    "IndexSeek",
    "NestedLoopJoin",
    "ChoosePlan",
    "HashGroupBy",
    "StreamingGroupBy"
  ).map(_.toLowerCase)

  private def coalescePartitions(partitions: Seq[SinglestorePartition],
                                 numPartitions: Int): Array[Partition] = {
    partitions
      .groupBy(p => p.index % numPartitions)
      .toArray
      .sortBy(p => p._1)
      .map(p => SinglestoreMultiPartition(p._1, p._2))
  }

  def saveErrorMessage(parallelReadType: ParallelReadType, message: String): Unit = {
    errorMessages += (parallelReadType -> message)
    if (log.isTraceEnabled()) {
      log.trace(s"$parallelReadType disabled for this query: $message")
    }
  }

  private def partitionsFromExplainJSON(database: String,
                                        partitionHostPorts: List[SinglestorePartitionInfo],
                                        explainJSON: JsValue): Option[Array[Partition]] = {
    def saveErrorMessageReadFromLeaves(message: String): Unit = {
      saveErrorMessage(ReadFromLeaves, message)
    }

    // The top level Explain is either the explain tree or a "metadata" wrapper
    // (we can disambiguate between these cases by checking for the presence of the "version" key)

    var root       = explainJSON
    val rootFields = explainJSON.asJsObject().fields
    if (rootFields.contains("version")) {
      // In explain version 2+ (introduced in SingleStore 6.7), the output
      // we want is nested under the "explain" key as the only element of an array
      root = rootFields("explain").convertTo[List[JsObject]].head
    }

    // Collect required execution metadata
    // We need to walk through the tree and collect the "executor" and "query" fields from each node

    def walk(node: JsValue): Seq[(Option[String], Option[String])] = {
      val fields   = node.asJsObject.fields
      val executor = fields.get("executor").map(_.convertTo[String].toLowerCase)
      val query    = fields.get("query").map(_.convertTo[String])
      val children =
        fields.get("inputs").map(_.convertTo[Seq[JsValue]].flatMap(walk)).getOrElse(Nil)
      Seq((executor, query)) ++ children
    }

    val (maybeExecutors, maybeQueries) = walk(root).unzip
    val executors                      = maybeExecutors.flatten
    val queries                        = maybeQueries.flatten

    // We are able to pushdown when the following conditions hold:
    // 1. there is exactly one query
    if (queries.length != 1) {
      saveErrorMessageReadFromLeaves(
        s"SingleStore would run more than one query during execution (${queries.length} queries found)")
      return None
    }

    // 2. all of the executors are in our whitelist
    if (executors.map(!executorWhitelist.contains(_)).exists(identity)) {
      saveErrorMessageReadFromLeaves(
        s"SingleStore is using parallel-unsafe executors (distinct executors in use: ${executors.toSet
          .mkString(", ")})")
      return None
    }

    // 3. there is only one gather, and it is the first executor
    val numGathers  = executors.count(_ == "gather")
    val gatherFirst = executors.headOption.contains("gather")
    if (numGathers != 1 || !gatherFirst) {
      saveErrorMessageReadFromLeaves(
        s"the gather method used by this query is not supported ($numGathers, $gatherFirst)")
      return None
    }

    // 4. all leaves are connectable
    if (partitionHostPorts.exists(p =>
          Try {
            SinglestoreConnectionPool
              .getConnection(
                JdbcHelpers.getConnProperties(options, isOnExecutor = false, p.hostport))
              .close()
          }.isFailure)) {
      saveErrorMessageReadFromLeaves(s"some leaves are not connectable")
      return None
    }

    // Success! we can do partition pushdown, to do this we need to generate queries for each partition

    // we checked earlier that queries.length == 1, so we can safely grab the first query here
    var partitionQuery = queries.head
    // the partitionQuery may start with USING, so lets remove everything up to the first SELECT
    partitionQuery = partitionQuery.slice(partitionQuery.indexOf("SELECT"), partitionQuery.length)

    val firstPartitionName = s"${database}_0"

    val singlePartitions = partitionHostPorts
      .map(
        p =>
          SinglestorePartition(
            p.ordinal,
            partitionQuery.replace(firstPartitionName, p.name),
            // SingleStore has already injected our variables into the query
            // so we don't have to do any additional injection
            Nil,
            JdbcHelpers.getConnProperties(options, isOnExecutor = true, p.hostport)
        ))

    val partitionsNum =
      if (options.parallelReadNumPartitions > 0) {
        singlePartitions.length.min(options.parallelReadNumPartitions)
      } else if (options.parallelReadMaxNumPartitions > 0) {
        singlePartitions.length.min(options.parallelReadMaxNumPartitions)
      } else {
        singlePartitions.length
      }

    Some(coalescePartitions(singlePartitions, partitionsNum))
  }

  private lazy val databasePartitionCount: Int = {
    val conn =
      SinglestoreConnectionPool.getConnection(getDMLConnProperties(options, isOnExecutor = false))
    try {
      JdbcHelpers.getPartitionsCount(conn, options.database.get)
    } finally {
      conn.close()
    }
  }

  private def aggregatorReadPartitions(partitionsNum: Int): Some[Array[Partition]] = {
    val singlePartitions = Seq
      .range(0, databasePartitionCount)
      .map(
        index =>
          SinglestorePartition(index,
                               rdd.query,
                               rdd.variables,
                               getDMLConnProperties(options, isOnExecutor = true)))

    Some(coalescePartitions(singlePartitions, partitionsNum))
  }

  private lazy val readFromLeavesPartitions: Option[Array[Partition]] = {
    val minimalExternalHostVersion = "7.1.0"
    val explainJSON =
      JdbcHelpers.explainJSONQuery(options, rdd.query, rdd.variables).parseJson
    val partitions = JdbcHelpers.partitionHostPorts(options, options.database.head)
    val partitionHostPorts = {
      val singlestoreVersion = SinglestoreVersion(JdbcHelpers.getSinglestoreVersion(options))
      if (singlestoreVersion.atLeast(minimalExternalHostVersion)) {
        val externalHostMap = JdbcHelpers.externalHostPorts(options)
        var isValid         = true
        val externalPartitions = partitions.flatMap(p => {
          val externalHost = externalHostMap.get(p.hostport)
          if (externalHost.isDefined) {
            Some(SinglestorePartitionInfo(p.ordinal, p.name, externalHost.get))
          } else {
            isValid = false
            None
          }
        })
        if (isValid) externalPartitions
        else partitions
      } else {
        partitions
      }
    }
    try {
      partitionsFromExplainJSON(options.database.head, partitionHostPorts, explainJSON)
    } catch {
      case _: DeserializationException => None
    }
  }

  private lazy val readFromAggregatorsMaterializedPartitions: Option[Array[Partition]] = {
    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(options, isOnExecutor = true))

    if (!JdbcHelpers.isValidQuery(
          conn,
          JdbcHelpers.getCreateResultTableQuery(
            "CheckIfSelectQueryIsSupportedByParallelRead",
            rdd.query,
            rdd.schema,
            materialized = true,
            needsRepartition = rdd.options.parallelReadRepartition,
            rdd.parallelReadRepartitionColumns
          ),
          rdd.variables
        )) {
      saveErrorMessage(ReadFromAggregatorsMaterialized,
                       "the query is not supported by aggregator parallel read")
      None
    } else {
      val numPartitions = if (options.parallelReadNumPartitions > 0) {
        databasePartitionCount.min(options.parallelReadNumPartitions)
      } else if (options.parallelReadMaxNumPartitions > 0) {
        databasePartitionCount.min(options.parallelReadMaxNumPartitions)
      } else {
        databasePartitionCount
      }

      aggregatorReadPartitions(numPartitions)
    }
  }

  private lazy val readFromAggregatorsPartitions: Option[Array[Partition]] = {
    def saveErrorMessageReadFromAggregators(message: String): Unit = {
      saveErrorMessage(ReadFromAggregators, message)
    }

    val conn =
      SinglestoreConnectionPool.getConnection(getDDLConnProperties(options, isOnExecutor = true))
    val maxNumConcurrentTasks = MaxNumConcurrentTasks.get(rdd)

    if (!JdbcHelpers.isValidQuery(
          conn,
          JdbcHelpers.getCreateResultTableQuery(
            "CheckIfSelectQueryIsSupportedByParallelRead",
            rdd.query,
            rdd.schema,
            materialized = false,
            needsRepartition = rdd.options.parallelReadRepartition,
            rdd.parallelReadRepartitionColumns
          ),
          rdd.variables
        )) {
      saveErrorMessageReadFromAggregators("the query is not supported by aggregator parallel read")
      None
    } else if (options.parallelReadNumPartitions == 0 && maxNumConcurrentTasks == 0) {
      saveErrorMessageReadFromAggregators(
        "failed to retrieve maximum number of concurrent tasks and number of partitions is not specified.\n" +
          "Try to set `parallelRead.numPartitions` parameter.")
      None
    } else {
      val numPartitions = if (options.parallelReadNumPartitions > 0) {
        databasePartitionCount
          .min(options.parallelReadNumPartitions)
      } else if (options.parallelReadMaxNumPartitions > 0) {
        databasePartitionCount
          .min(MaxNumConcurrentTasks.get(rdd))
          .min(options.parallelReadMaxNumPartitions)
      } else {
        databasePartitionCount
          .min(MaxNumConcurrentTasks.get(rdd))
      }

      aggregatorReadPartitions(numPartitions)
    }
  }

  private lazy val nonParallelReadPartitions: Option[Array[Partition]] =
    Some(
      coalescePartitions(
        Seq(
          SinglestorePartition(0,
                               rdd.query,
                               rdd.variables,
                               getDMLConnProperties(options, isOnExecutor = true))),
        1))

  private def getPartitions(parallelReadType: ParallelReadType): Option[Array[Partition]] =
    if (options.database.isEmpty) {
      None
    } else {
      parallelReadType match {
        case ReadFromLeaves                  => readFromLeavesPartitions
        case ReadFromAggregators             => readFromAggregatorsPartitions
        case ReadFromAggregatorsMaterialized => readFromAggregatorsMaterializedPartitions
        case _                               => None
      }
    }

  def getPartitions: (Option[ParallelReadType], Array[Partition]) = {
    val readType = options.enableParallelRead match {
      case Disabled =>
        None
      case AutomaticLite if rdd.resultMustBeSorted =>
        None
      case _ =>
        rdd.options.parallelReadFeatures
          .collectFirst { case readType if getPartitions(readType).isDefined => readType }
    }

    if (readType.isEmpty && options.enableParallelRead == Forced) {
      throw new ParallelReadFailedException(errorMessages)
    }

    val partitions = readType match {
      case None                                  => nonParallelReadPartitions
      case Some(ReadFromLeaves)                  => readFromLeavesPartitions
      case Some(ReadFromAggregators)             => readFromAggregatorsPartitions
      case Some(ReadFromAggregatorsMaterialized) => readFromAggregatorsMaterializedPartitions
    }

    (readType, partitions.get)
  }
}

final class ParallelReadFailedException(errors: Map[ParallelReadType, String])
    extends SQLException(
      s"Failed to read data in parallel.\n" +
        s"Tried following parallel read features:\n" +
        s"${errors
          .map(error => s" * ${error._1} - ${error._2}\n")
          .mkString("")}")
