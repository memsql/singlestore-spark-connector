import xerial.sbt.Sonatype._

/*
  To run tests or publish with a specific spark version use this java option:
    -Dspark.version=3.0.0
 */
val sparkVersion = sys.props.get("spark.version").getOrElse("4.0.0")
val scalaVersionStr = sparkVersion match {
  case "3.1.3" => "2.12.12"
  case "3.2.4" => "2.12.12"
  case "3.3.4" => "2.12.12"
  case "3.4.2" => "2.12.12"
  case "3.5.0" => "2.12.12"
  case "4.0.0" => "2.13.8"
}
val scalaVersionPrefix = scalaVersionStr.substring(0, 4)
val jacksonDatabindVersion = sparkVersion match {
  case "3.1.3" => "2.10.0"
  case "3.2.4" => "2.12.3"
  case "3.3.4" => "2.13.4.2"
  case "3.4.2" => "2.14.2"
  case "3.5.0" => "2.15.2"
  case "4.0.0" => "2.18.2"
}

lazy val root = project
  .withId("singlestore-spark-connector")
  .in(file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "singlestore-spark-connector",
    organization := "com.singlestore",
    scalaVersion := scalaVersionStr,
    Compile / unmanagedSourceDirectories += (Compile / sourceDirectory).value / (sparkVersion match {
      case "3.1.3" => "scala-sparkv3.1"
      case "3.2.4" => "scala-sparkv3.2"
      case "3.3.4" => "scala-sparkv3.3"
      case "3.4.2" => "scala-sparkv3.4"
      case "3.5.0" => "scala-sparkv3.5"
      case "4.0.0" => "scala-sparkv4.0"
    }),
    version := s"4.1.11-spark-${sparkVersion}",
    licenses += "Apache-2.0" -> url(
      "http://opensource.org/licenses/Apache-2.0"
    ),
    resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
    libraryDependencies ++= Seq(
      // runtime dependencies
      "org.apache.spark"       %% "spark-core"             % sparkVersion % "provided, test",
      "org.apache.spark"       %% "spark-sql"              % sparkVersion % "provided, test",
      "org.apache.avro"        % "avro"                    % "1.11.3",
      "org.apache.commons"     % "commons-dbcp2"           % "2.7.0",
      "org.scala-lang.modules" %% "scala-java8-compat"     % "0.9.0",
      "com.singlestore"        % "singlestore-jdbc-client" % "1.2.7",
      "io.spray"               %% "spray-json"             % "1.3.5",
      "io.netty"               % "netty-buffer"            % "4.1.70.Final",
      "org.apache.commons"     % "commons-dbcp2"           % "2.9.0",
      // test dependencies
      "org.mariadb.jdbc"    % "mariadb-java-client" % "2.+"     % Test,
      "org.scalatest"       %% "scalatest"          % "3.1.0"   % Test,
      "org.scalacheck"      %% "scalacheck"         % "1.14.1"  % Test,
      "org.mockito"         %% "mockito-scala"      % "1.16.37" % Test,
      "com.github.mrpowers" %% "spark-fast-tests"   % "1.1.0"   % Test,
      "com.github.mrpowers" %% "spark-daria"        % "1.2.3"  % Test
    ),
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabindVersion,
    Test / testOptions += Tests.Argument("-oF"),
    Test / fork := true,
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "com.singlestore.spark"
  )

credentials += Credentials(
  "GnuPG Key ID",
  "gpg",
  "CDD996495CF08BB2041D86D8D1EB3D14F1CD334F",
  "ignored" // this field is ignored; passwords are supplied by pinentry
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

publishTo := sonatypePublishToBundle.value
publishMavenStyle := true
sonatypeSessionName := s"[sbt-sonatype] ${name.value} ${version.value}"
sonatypeProjectHosting := Some(GitHubHosting("memsql", "memsql-spark-connector", "carl@memsql.com"))
