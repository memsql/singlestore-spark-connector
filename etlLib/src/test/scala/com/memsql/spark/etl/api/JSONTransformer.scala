package com.memsql.spark.etl.api

import com.memsql.spark.etl.utils._

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.`type`.TypeReference;
import com.fasterxml.jackson.core._
import org.scalatest._

class TestJSONPath extends FlatSpec {
  def testPathExtract(path: JSONPath, json: String) : String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    path.get(mapper.readValue(json, classOf[Map[String,Any]]), mapper)
  }

  "JSONPath" should "extract the path" in {
    assert(testPathExtract(JSONPath("a"),"""{}""") == null)
    
    assert(testPathExtract(JSONPath("a"),    """{"b":1}""") == null)
    assert(testPathExtract(JSONPath("b","a"),"""{"b":1}""") == null)
    assert(testPathExtract(JSONPath("a","b"),"""{"b":1}""") == null)
    
    assert(testPathExtract(JSONPath("a"),"""{"a":1}""") == "1")
    assert(testPathExtract(JSONPath("a"),"""{"a":"gbop"}""") == "gbop")
    assert(testPathExtract(JSONPath("a"),"""{"a":[1,2,3,4]}""") == "[1,2,3,4]")
    assert(testPathExtract(JSONPath("a"),"""{"a":[1,2,3,{"a":1}]}""") == """[1,2,3,{"a":1}]""")
    assert(testPathExtract(JSONPath("a","1"),"""{"a":[1,2,3,4]}""") == null)
    assert(testPathExtract(JSONPath("a","b"),"""{"a":[1,2,3,4]}""") == null)

    
    assert(testPathExtract(JSONPath("a","b"),    """{"a": { "b" : 1 } }""") == "1")
    assert(testPathExtract(JSONPath("a","b"),    """{"a": { "b" : "gbop" } }""") == "gbop")
    assert(testPathExtract(JSONPath("a","b","c"),"""{"a": { "b" : 1 } }""") == null)
    assert(testPathExtract(JSONPath("b","a"),    """{"a": { "b" : 1 } }""") == null)
    assert(testPathExtract(JSONPath("a","a"),    """{"a": { "b" : 1 } }""") == null)
    assert(testPathExtract(JSONPath("a"),        """{"a": { "b" : 1 } }""") == """{"b":1}""")
    
    assert(testPathExtract(JSONPath("a","a"),    """{"a": { "a" : 1 } }""") == "1")
    assert(testPathExtract(JSONPath("a","a"),    """{"a": { "a" : "gbop" } }""") == "gbop")
    assert(testPathExtract(JSONPath("a","a","a"),"""{"a": { "a" : 1 } }""") == null)
    assert(testPathExtract(JSONPath("a","b"),    """{"a": { "a" : 1 } }""") == null)
    
    assert(testPathExtract(JSONPath("a","b","c"),"""{"a": { "b" : {"c" : 1 } } }""") == "1")
    assert(testPathExtract(JSONPath("a"),        """{"a": { "b" : {"c" : 1 } } }""") == """{"b":{"c":1}}""")
    assert(testPathExtract(JSONPath("a","b","c"),"""{"a": { "b" : {"c" : "gbop" } } }""") == "gbop")
  }
  
}
