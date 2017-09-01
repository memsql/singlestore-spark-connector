lazy val sparkVersion = "2.0.2"
lazy val mysqlConnectorVersion = "5.1.34"
lazy val sprayVersion = "1.3.2"
lazy val scalatestVersion = "2.2.5"
lazy val commonsDBCPVersion = "2.1.1"
lazy val guavaVersion = "19.0"

lazy val assemblyScalastyle = taskKey[Unit]("assemblyScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val commonSettings = Seq(
  organization := "com.memsql",
  version := "2.0.4",
  scalaVersion := "2.11.8",
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
    <url>http://memsql.github.io/memsql-spark-connector</url>
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
  excludeFilter in unmanagedSources := HiddenFileFilter || "prelude.scala"
)

lazy val examples = (project in file("examples")).
  dependsOn(root % "compile->test; test->test").
  settings(commonSettings: _*).
  settings(
    name := "examples",
    libraryDependencies ++= {
      Seq(
        "mysql" % "mysql-connector-java" % mysqlConnectorVersion,
        "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
        "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided
      )
    }
  )

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(site.settings ++ ghpages.settings: _*).
  settings(
    name := "MemSQL-Connector",
    description := "Spark MemSQL Connector",
    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
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
    },
    site.includeScaladoc(),
    git.remoteRepo := s"git@github.com:memsql/memsql-spark-connector.git"
  )
