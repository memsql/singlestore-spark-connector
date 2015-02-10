lazy val connectorLib = (project in file("connectorLib")).
  settings(
    name := "MemSQLRDD",
    version := "0.1.2",
    scalaVersion := "2.10.4",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.1.0" % "provided",
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
        findManagedDependency("org.apache.spark", "spark-core").map(d => d -> url("https://spark.apache.org/docs/1.1.0/api/scala/"))
      )
      links.collect { case Some(d) => d }.toMap
    }
  )

lazy val root = (project in file(".")).
  dependsOn(connectorLib).
  settings(
    name := "MemSQLRDDApp",
    version := "0.1.2",
    scalaVersion := "2.10.4",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.1.0" % "provided",
      "mysql" % "mysql-connector-java" % "5.1.34"
    )
  )
