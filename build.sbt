import xerial.sbt.Sonatype._

/*
  To run tests or publish with a specific spark version use this java option:
    -Dspark.version=3.0.0
 */

// update this version when picking up a new Flame release
val aiqSparkVersion    = "3-3-2-aiq113"

val sparkVersion       = aiqSparkVersion.substring(0,5).replace("-", ".")
val scalaVersionStr    = "2.12.15"
val scalaVersionPrefix = scalaVersionStr.substring(0, 4)
val jacksonDatabindVersion = sparkVersion match {
  case "3.1.3"           => "2.10.0"
  case "3.2.4"           => "2.12.3"
  case "3.3.2" | "3.3.4" => "2.13.4.2"
  case "3.4.2"           => "2.14.2"
  case "3.5.0"           => "2.15.2"
}

// increment this version when making a new release
val sparkConnectorVersion = "4.1.8-aiq1"

lazy val root = project
  .withId("singlestore-spark-connector")
  .in(file("."))
  .enablePlugins(PublishToArtifactory, BuildInfoPlugin)
  .settings(
    name := "singlestore-spark-connector",
    organization := "com.singlestore",
    scalaVersion := scalaVersionStr,
    scalacOptions ++= Seq("-release", "17"),
    javacOptions ++= Seq("-source", "17", "-target", "17"),
    javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.math=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ),
    Compile / unmanagedSourceDirectories += (Compile / sourceDirectory).value / (sparkVersion match {
      case "3.1.3"           => "scala-sparkv3.1"
      case "3.2.4"           => "scala-sparkv3.2"
      case "3.3.2" | "3.3.4" => "scala-sparkv3.3"
      case "3.4.2"           => "scala-sparkv3.4"
      case "3.5.0"           => "scala-sparkv3.5"
    }),
    version := s"${sparkConnectorVersion}-spark-${sparkVersion}",
    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    resolvers ++= Seq(
      "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
      "aiq-artifacts".at("s3://s3-us-east-1.amazonaws.com/aiq-artifacts/releases"),
      "Artifactory".at("https://actioniq.jfrog.io/artifactory/aiq-sbt-local/"),
      DefaultMavenRepository,
      Resolver.mavenLocal,
    ),
    libraryDependencies ++= Seq(
      // runtime dependencies
      "org.apache.spark"       %% "spark-core"             % aiqSparkVersion % "provided, test",
      "org.apache.spark"       %% "spark-sql"              % aiqSparkVersion % "provided, test",
      "org.apache.avro"        % "avro"                    % "1.11.3",
      "org.apache.commons"     % "commons-dbcp2"           % "2.7.0",
      "org.scala-lang.modules" %% "scala-java8-compat"     % "0.9.0",
      "com.singlestore"        % "singlestore-jdbc-client" % "1.2.4",
      "io.spray"               %% "spray-json"             % "1.3.5",
      "io.netty"               % "netty-buffer"            % "4.1.70.Final",
      "org.apache.commons"     % "commons-dbcp2"           % "2.9.0",
      // test dependencies
      "org.mariadb.jdbc"    % "mariadb-java-client" % "2.+"     % Test,
      "org.scalatest"       %% "scalatest"          % "3.1.0"   % Test,
      "org.scalacheck"      %% "scalacheck"         % "1.14.1"  % Test,
      "org.mockito"         %% "mockito-scala"      % "1.16.37" % Test,
      "com.github.mrpowers" %% "spark-fast-tests"   % "0.21.3"  % Test,
      "com.github.mrpowers" %% "spark-daria"        % "0.38.2"  % Test
    ),
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabindVersion,
    Test / testOptions += Tests.Argument("-oF"),
    Test / fork := true,
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "com.singlestore.spark"
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
