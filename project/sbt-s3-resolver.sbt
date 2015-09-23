lazy val s3Plugin = uri("git://github.com/ohnosequences/sbt-s3-resolver#v0.13.0")

lazy val root = (project in file(".")).dependsOn(s3Plugin)
