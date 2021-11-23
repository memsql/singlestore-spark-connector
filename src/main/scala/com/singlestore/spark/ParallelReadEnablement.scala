package com.singlestore.spark

sealed trait ParallelReadEnablement

case object Disabled      extends ParallelReadEnablement
case object Automatic     extends ParallelReadEnablement
case object AutomaticLite extends ParallelReadEnablement
case object Forced        extends ParallelReadEnablement

object ParallelReadEnablement {
  def apply(value: String): ParallelReadEnablement = value.toLowerCase match {
    case "disabled"      => Disabled
    case "automaticlite" => AutomaticLite
    case "automatic"     => Automatic
    case "forced"        => Forced

    // These two options are added for compatibility purposes
    case "false" => Disabled
    case "true"  => Automatic

    case _ =>
      throw new IllegalArgumentException(
        s"""Illegal argument for `${SinglestoreOptions.ENABLE_PARALLEL_READ}` option. Valid arguments are:
           | - "Disabled"
           | - "AutomaticLite"
           | - "Automatic"
           | - "Forced"""".stripMargin)
  }
}
