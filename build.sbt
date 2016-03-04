import UnidocKeys._

lazy val sparkVersion = "1.5.1"
lazy val mysqlConnectorVersion = "5.1.34"
lazy val akkaVersion = "2.3.9"
lazy val sprayVersion = "1.3.2"
lazy val scoptVersion = "3.2.0"
lazy val scalatestVersion = "2.2.5"
lazy val commonsDBCPVersion = "2.1.1"
lazy val guavaVersion = "19.0"

lazy val assemblyScalastyle = taskKey[Unit]("assemblyScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val commonSettings = Seq(
  organization := "com.memsql",
  version := "1.2.2",
  scalaVersion := "2.10.5",
  assemblyScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value,
  assembly <<= assembly dependsOn assemblyScalastyle,
  testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value,
  (test in Test) <<= (test in Test) dependsOn testScalastyle,
  parallelExecution in Test := false,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) {
      Some("snapshots" at nexus + "content/repositories/snapshots")
    } else {
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
    }
  },
  pomExtra := {
    <url>http://memsql.github.io/spark-streamliner</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/memsql/memsql-spark-connector.git</connection>
      <developerConnection>scm:git:git@github.com:memsql/memsql-spark-connector.git</developerConnection>
      <url>github.com/memsql/memsql-spark-connector</url>
    </scm>
    <developers>
      <developer>
        <name>MemSQL</name>
        <email>ops@memsql.com</email>
        <url>http://www.memsql.com</url>
        <organization>MemSQL</organization>
        <organizationUrl>http://www.memsql.com</organizationUrl>
      </developer>
    </developers>
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  javaVersionPrefix in javaVersionCheck := Some("1.7"),
  excludeFilter in unmanagedSources := HiddenFileFilter || "prelude.scala"
)

lazy val common = (project in file("common")).
  settings(commonSettings: _*).
  settings(
    name := "common"
  )

lazy val connectorLib = (project in file("connectorLib")).
  settings(commonSettings: _*).
  settings(
    name := "MemSQL-Connector",
    description := "Spark MemSQL Connector",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided,
      "mysql" % "mysql-connector-java" % mysqlConnectorVersion,
      "org.apache.commons" % "commons-dbcp2" % commonsDBCPVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "com.google.guava" % "guava" % guavaVersion,
      "io.spray" %% "spray-json" % sprayVersion
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
        findManagedDependency("org.apache.spark", "spark-core").map(d => d -> url(s"https://spark.apache.org/docs/$sparkVersion/api/scala/")),
        findManagedDependency("org.apache.spark", "spark-sql").map(d => d -> url(s"https://spark.apache.org/docs/$sparkVersion/api/scala/"))
      )
      links.collect { case Some(d) => d }.toMap
    }
  )

lazy val etlLib = (project in file("etlLib")).
  dependsOn(connectorLib).
  settings(commonSettings: _*).
  settings(
    name := "MemSQL ETL",
    description := "Spark MemSQL ETL Library",
    libraryDependencies  ++= Seq(
      "io.spray" %% "spray-json" % sprayVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
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
        findManagedDependency("org.apache.spark", "spark-core").map(d => d -> url(s"https://spark.apache.org/docs/$sparkVersion/api/scala/")),
        findManagedDependency("org.apache.spark", "spark-streaming").map(d => d -> url(s"https://spark.apache.org/docs/$sparkVersion/api/scala/")),
        findManagedDependency("org.apache.spark", "spark-sql").map(d => d -> url(s"https://spark.apache.org/docs/$sparkVersion/api/scala/"))
      )
      links.collect { case Some(d) => d }.toMap
    },
    resourceGenerators in Compile += Def.task {
      val file = (resourceManaged in Compile).value / "MemSQLETLVersion"
      IO.write(file, version.value)
      Seq(file)
    }.taskValue
  )

lazy val jarInspector = (project in file("jarInspector")).
  dependsOn(etlLib).
  settings(commonSettings: _*).
  settings(
    name := "jarInspector",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "io.spray" %% "spray-json" % sprayVersion,
      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.reflections" % "reflections" % "0.9.10"
    )
  )

lazy val hdfsUtils = (project in file("hdfsUtils")).
  settings(commonSettings: _*).
  settings(
    name := "hdfsUtils",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "io.spray" %% "spray-json" % sprayVersion,
      "com.github.scopt" %% "scopt" % scoptVersion
    )
  )

lazy val samplingUtils = (project in file("samplingUtils")).
  settings(commonSettings: _*).
  dependsOn(common).
  settings(
    name := "samplingUtils",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided,
      "io.spray" %% "spray-json" % sprayVersion,
      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.apache.commons" % "commons-csv" % "1.2"
    )
  )

lazy val interface = (project in file("interface")).
  dependsOn(connectorLib % "test->test;compile->compile").
  dependsOn(etlLib % "test->test;compile->compile").
  dependsOn(common).
  dependsOn(jarInspector).
  dependsOn(hdfsUtils).
  dependsOn(samplingUtils).
  settings(commonSettings: _*).
  enablePlugins(BuildInfoPlugin).
  settings(
    name := "MemSQL Spark Interface",
    buildInfoKeys := Seq[BuildInfoKey](version, BuildInfoKey.action("commitHash") {
        Process("git rev-parse HEAD").lines.head
    }),
    buildInfoPackage := "com.memsql.spark.interface.meta",
    libraryDependencies ++= {
      Seq(
        "io.spray" %% "spray-json" % sprayVersion,
        "io.spray" %% "spray-can" % sprayVersion,
        "io.spray" %% "spray-routing" % sprayVersion,
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.github.scopt" %% "scopt" % scoptVersion,
        "mysql" % "mysql-connector-java" % mysqlConnectorVersion,
        "org.eclipse.jetty" % "jetty-servlet" % "8.1.14.v20131031" % Provided,
        "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
        "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided,
        "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
        "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion exclude("org.spark-project.spark", "unused"),
        "org.scalatest" %% "scalatest" % scalatestVersion % Test,
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
        "io.spray" %% "spray-testkit" % sprayVersion % Test exclude("org.scalamacros", "quasiquotes_2.10.3"),
        "org.apache.commons" % "commons-csv" % "1.2"
      )
    }
  )

lazy val examples = (project in file("examples")).
  dependsOn(interface).
  settings(commonSettings: _*).
  settings(
    name := "examples",
    libraryDependencies ++= {
      Seq(
        "mysql" % "mysql-connector-java" % mysqlConnectorVersion,
        "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
        "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
        "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided
      )
    }
  )

lazy val tests = (project in file("tests")).
  dependsOn(connectorLib % "compile->test;test->test").
  dependsOn(etlLib).
  dependsOn(interface).
  dependsOn(examples).
  settings(commonSettings: _*).
  settings(
    name := "tests",
    libraryDependencies ++= {
      Seq(
        "mysql" % "mysql-connector-java" % mysqlConnectorVersion,
        "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
        "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
        "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided
      )
    }
  )

lazy val root = (project in file(".")).
  dependsOn(connectorLib).
  dependsOn(etlLib).
  dependsOn(interface).
  settings(commonSettings: _*).
  settings(unidocSettings: _*).
  settings(site.settings ++ ghpages.settings: _*).
  settings(
    name := "MemSQL",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
      "mysql" % "mysql-connector-java" % mysqlConnectorVersion
    ),
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(tests, jarInspector, hdfsUtils),
    site.includeScaladoc(),
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "latest/api"),
    git.remoteRepo := s"git@github.com:memsql/memsql-spark-connector.git"
  )
