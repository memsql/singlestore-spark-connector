package com.singlestore.spark

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.singlestore.spark.SQLGen.SinglestoreVersion
import org.apache.commons.dbcp2.{BasicDataSource, BasicDataSourceFactory}
import org.scalatest.funspec.AnyFunSpec

import scala.collection.mutable

class VersionTest extends AnyFunSpec {

  it("singlestore version test") {

    val conf = new SinglestoreOptions(
      s"localhost:5506",
      List.empty[String],
      "root",
      "1",
      None,
      Map.empty[String, String],
      false,
      false,
      Automatic,
      Truncate,
      SinglestoreOptions.CompressionType.GZip,
      SinglestoreOptions.LoadDataFormat.CSV,
      List.empty[SinglestoreOptions.TableKey],
      None,
      10,
      10,
      List(ReadFromLeaves),
      0,
      0,
      true,
      Set.empty
    )

    JdbcHelpers.getDMLConnProperties(conf)

    //    val dataSources = new mutable.HashMap[Properties, BasicDataSource]()
//    val p1          = new Properties()
//    p1.setProperty("asd", "asd1")
//    val p2 = new Properties()
//    p2.setProperty("asd", "asd1")
//    p2.setProperty("asd", "asd12")
//    dataSources += (p1 -> new BasicDataSource)
//    dataSources += (p2 -> new BasicDataSource)
//    println("AAAAAA " + dataSources.size)
//    val sessionVariables = Seq(
//      "collation_server=utf8_general_ci",
//      "sql_select_limit=18446744073709551615",
//      "compile_only=false",
//      "sql_mode='STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY'"
//    ).mkString(",")
//    val propText = s"a=1;b=2;c=${sessionVariables}"
//
//    val p3 = {
//      val p = new Properties
//      if (propText != null)
//        p.load(
//          new ByteArrayInputStream(
//            propText.replace(';', '\n').getBytes(StandardCharsets.ISO_8859_1)))
//      p
//    }
//
//    println(p3)
  }
}
