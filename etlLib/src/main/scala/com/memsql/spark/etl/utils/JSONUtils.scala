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

/*
 * Given a blob { "a" : { "b" : {"c" : "value" } } },
 * JSONPath("a","b","c").get(blob, mapper) will extract "value"
 */
object JSONPath {
  def apply(dataType : DataType, path : String*) : JSONPath = makeJSONPath(dataType, path.toArray)
  def apply(path: String*) : JSONPath = makeJSONPath(StringType, path.toArray)
  def makeJSONPath(dataType : DataType, path : Array[String]) : JSONPath = {
    val pathArray = path.reverse
    var result: JSONPath = JSONExtractLeaf(pathArray(0), dataType)
    for (i <- 1 until pathArray.size)
    {
        result = JSONExtractObject(pathArray(i), result)
    }
    result
  }

}

object JSONUtils {

  /**
   * equivalent to JSONRDDToDataFrame(flattenedPaths, sqlContext, rdd).rdd, but does not create dataframe
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
   * A utility which transforms a JSON RDD[String]
   * to a DataFrame of flattened JSON from a provided array of paths.
   *
   * NOTE: The resulting dataframe is suitable for loading to a target table that has additional columns with defaults
   * (including `TIMESTAMP default CURRENT_TIME` and computed columns).
   *
   * For instance, given JSON blobs of the form
   *    { "a" : value1,
   *      "b" : { "c" : value2
   *              "d" : value3
   *      }
   *    }
   * The paths
   *    JSONPath("a"),
   *    JSONPath("b","c"),
   *    JSONPath("b","d")
   * will produce a DataFrame like
   *    +--------+--------+--------+
   *    | a      | b_c    | b_d    |
   *    +--------+--------+--------+
   *    | value1 | value2 | value3 |
   *    +--------+--------+--------+
   *
   * For non-leaf-paths, you will get JSON as String
   * for instance,
   *    JSONPath("b")
   * will yield
   *    +--------+--------+---------+
   *    | b                         |
   *    +--------+--------+---------+
   *    | {"c":value1, "d":value2"} |
   *    +--------+--------+---------+
   *
   * Any nonexisting paths will yield null.
   * Unparseable JSON will throw a runtime com.xml.jackson.core.JsonParseException on the executors.
   * This Transformer currently does not support flattening JSON lists.
   *
   */
  def JSONRDDToDataFrame(flattenedPaths: Array[JSONPath], sqlContext: SQLContext, rdd: RDD[String]): DataFrame = {
    val transformedRDD: RDD[Row] = JSONRDDToRows(flattenedPaths, rdd)
    val schema = StructType(flattenedPaths.map((jp : JSONPath) => StructField(jp.getName, StringType, true)))
    sqlContext.createDataFrame(transformedRDD, schema)
  }
}
