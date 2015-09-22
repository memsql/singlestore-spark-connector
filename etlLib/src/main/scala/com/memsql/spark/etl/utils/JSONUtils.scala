package com.memsql.spark.etl.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

abstract class JSONPath {
  def get(obj: Map[String,Any], mapper: ObjectMapper): String
  def getName: String
  def getName(name: StringBuilder): StringBuilder
  def getType: DataType
}
case class JSONExtractObject(objName: String, pathTail: JSONPath) extends JSONPath {
  override def get(obj: Map[String,Any], mapper: ObjectMapper): String = {
    obj.getOrElse(objName, null) match {
      case nextObj: Map[_,_] => pathTail.get(nextObj.asInstanceOf[Map[String,Any]], mapper)
      case _ => null
    }
  }
  override def getName: String = {
    val name = new StringBuilder
    getName(name).toString
  }
  override def getName(name: StringBuilder): StringBuilder = {
    name.append(objName).append("_")
    pathTail.getName(name)
  }
  override def getType: DataType = pathTail.getType
}
case class JSONExtractLeaf(leafName: String, leafType: DataType) extends JSONPath {
  override def get(obj: Map[String,Any], mapper: ObjectMapper): String = {
    obj.getOrElse(leafName, null) match {
      case null => null
      case obj @(_ : Map[_,_] | _ : List[_]) => mapper.writeValueAsString(obj)
      case other => other.toString
    }
  }
  override def getName: String = leafName
  override def getName(name: StringBuilder): StringBuilder = name.append(leafName)
  override def getType: DataType = leafType
}

/**
 * Utility for extracting values from a JSON blob.
 *
 * {{{
 * //Given a blob { "a" : { "b" : {"c" : "value" } } },
 * JSONPath("a","b","c").get(blob, mapper) == "value"
 * }}}
 */
object JSONPath {
  def apply(dataType: DataType, path: String*): JSONPath = makeJSONPath(dataType, path.toArray)
  def apply(path: String*): JSONPath = makeJSONPath(StringType, path.toArray)

  /**
   *
   * @param dataType
   * @param path
   * @return
   */
  def makeJSONPath(dataType: DataType, path: Array[String]): JSONPath = {
    val pathArray = path.reverse
    var result: JSONPath = JSONExtractLeaf(pathArray(0), dataType)
    for (i <- 1 until pathArray.size) {
        result = JSONExtractObject(pathArray(i), result)
    }
    result
  }
}

/**
 * Utility for parsing a JSON formatted [[org.apache.spark.rdd.RDD]].
 */
object JSONUtils {
  /**
   * Equivalent to [[JSONRDDToDataFrame]].rdd, but does not create a [[org.apache.spark.sql.DataFrame]].
   *
   * @param flattenedPaths An [[scala.Array]] of [[JSONPath]]s to extract.
   * @param rdd The [[org.apache.spark.rdd.RDD]] to parse as JSON.
   */
  def JSONRDDToRows(flattenedPaths: Array[JSONPath], rdd: RDD[String]): RDD[Row] = {
    rdd.mapPartitions{ part =>
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      part.map { r =>
        val json = mapper.readValue(r, classOf[Map[String,Any]])
        Row.fromSeq(flattenedPaths.map(_.get(json, mapper)))
      }
    }
  }

  /**
   * A utility which transforms a JSON [[org.apache.spark.rdd.RDD]] to a [[org.apache.spark.sql.DataFrame]]
   * of flattened JSON from a provided array of [[JSONPath]]s.
   *
   * NOTE: The resulting [[org.apache.spark.sql.DataFrame]] is suitable for loading to a target table that has
   * additional columns with defaults (including `TIMESTAMP default CURRENT_TIME` and computed columns).
   *
   * For instance, given JSON blobs of the form
   *    {
   *      "a" : value1,
   *      "b" : {
   *        "c" : value2,
   *        "d" : value3
   *      }
   *    }
   * The paths
   * {{{
   * Array(JSONPath("a"), JSONPath("b","c"), JSONPath("b","d"))
   * }}}
   * will produce a DataFrame like
   * <pre>
   *    +--------+--------+--------+
   *    | a      | b_c    | b_d    |
   *    +--------+--------+--------+
   *    | value1 | value2 | value3 |
   *    +--------+--------+--------+
   * </pre>
   *
   * For non-leaf-paths, you will get the flattened JSON as [[scala.Predef.String]].
   * for instance,
   * {{{
   * Array(JSONPath("b"))
   * }}}
   * will yield
   * <pre>
   *    +--------+--------+---------+
   *    | b                         |
   *    +--------+--------+---------+
   *    | {"c":value1, "d":value2"} |
   *    +--------+--------+---------+
   * </pre>
   *
   * Any nonexisting paths will yield null.
   * Malformed JSON will throw a runtime [[com.fasterxml.jackson.core.JsonParseException]] on the executors.
   * This utility currently does not support flattening JSON arrays.
   */
  def JSONRDDToDataFrame(flattenedPaths: Array[JSONPath], sqlContext: SQLContext, rdd: RDD[String]): DataFrame = {
    val transformedRDD: RDD[Row] = JSONRDDToRows(flattenedPaths, rdd)
    val schema = StructType(flattenedPaths.map((jp: JSONPath) => StructField(jp.getName, StringType, true)))
    sqlContext.createDataFrame(transformedRDD, schema)
  }
}
