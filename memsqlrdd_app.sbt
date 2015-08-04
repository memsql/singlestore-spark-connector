lazy val commonSettings = Seq(
  organization := "com.memsql",
  version := "0.1.3-SNAPSHOT",
  scalaVersion := "2.10.4"
)

lazy val connectorLib = (project in file("connectorLib")).
  settings(commonSettings: _*).
  settings(
    name := "memsql-spark-connector",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
      "mysql" % "mysql-connector-java" % "5.1.34"
    ),
    autoAPIMappings := true,
    publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository"))),
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
  settings(commonSettings: _*).
  settings(
    name := "MemSQLRDDApp",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
      "mysql" % "mysql-connector-java" % "5.1.34"
    )
  )
