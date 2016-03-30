resolvers += Resolver.sonatypeRepo("snapshots")

lazy val root = (project in file(".")).
  settings(
    name := "sample-pipelines",
    version := "0.0.1",
    scalaVersion := "2.10.5",
    libraryDependencies  ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
        "org.apache.spark" %% "spark-sql" % "1.5.2"  % "provided",
        "org.apache.spark" %% "spark-streaming" % "1.5.2" % "provided",
        "com.memsql" %% "memsql-etl" % "1.3.1-SNAPSHOT"
    )
)
