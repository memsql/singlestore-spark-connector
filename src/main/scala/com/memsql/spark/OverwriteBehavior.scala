package com.memsql.spark

sealed trait OverwriteBehavior

case object Truncate      extends OverwriteBehavior
case object Merge         extends OverwriteBehavior
case object DropAndCreate extends OverwriteBehavior

object OverwriteBehavior {
  def apply(value: String): OverwriteBehavior = value.toLowerCase match {
    case "truncate"      => Truncate
    case "merge"         => Merge
    case "dropandcreate" => DropAndCreate
    case _ =>
      throw new IllegalArgumentException(
        s"Illegal argument for `${MemsqlOptions.OVERWRITE_BEHAVIOR}` option")
  }
}
