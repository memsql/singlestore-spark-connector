lazy val commonSettings = Seq(
  organization := "com.memsql",
  version := "0.1.2",
  scalaVersion := "2.10.4"
)

lazy val connectorLib = (project in file("connectorLib")).
  settings(commonSettings: _*).
  settings(
    name := "MemSQLRDD",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "1.4.0"  % "provided", 
      "mysql" % "mysql-connector-java" % "5.1.34"
    ),
    autoAPIMappings := true,
    apiMappings ++= {
      def findManagedDependency(organization: String, name: String): Option[File] = {
        (for {
          entry <- (fullClasspath in Runtime).value ++ (fullClasspath in Test).value
          module <- entry.get(moduleID.key) if module.organization == organization && module.name.startsWith(name)
        } yield entry.data).headOption
      }
      val links = Seq(
        findManagedDependency("org.apache.spark", "spark-core").map(d => d -> url("https://spark.apache.org/docs/1.4.0/api/scala/"))
      )
      links.collect { case Some(d) => d }.toMap
    }
  )

lazy val etlLib = (project in file("etlLib")).
  dependsOn(connectorLib).
  settings(commonSettings: _*).
  settings(
    name := "MemSQLPipeline",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "1.4.0" % "provided",
      "org.apache.spark" %% "spark-streaming-kafka" % "1.4.0" % "provided",
      "org.apache.kafka" %% "kafka" % "0.8.2.1"
    )
  )

lazy val root = (project in file(".")).
  dependsOn(connectorLib).
  dependsOn(etlLib).
  settings(commonSettings: _*).
  settings(
    name := "MemSQLRDDApp",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "1.4.0"  % "provided", 
      "mysql" % "mysql-connector-java" % "5.1.34"
    )
  )
