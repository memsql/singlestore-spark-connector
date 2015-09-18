resolvers += "memsql-internal" at "http://coreos-10.memcompute.com:8080/repository/internal"

lazy val root = (project in file(".")).
  settings(
    name := "sample-pipelines",
    version := "0.0.1",
    scalaVersion := "2.10.5",
    libraryDependencies  ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
        "org.apache.spark" %% "spark-sql" % "1.4.1"  % "provided",
        "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided",
        "com.memsql" %% "memsqletl" % "0.2.1"
    )
)
