import xerial.sbt.Sonatype._

/*
  To run tests or publish with a specific spark version use this java option:
    -Dspark.version=3.0.0
 */
val sparkVersion       = sys.props.get("spark.version").getOrElse("3.2.1")
val scalaVersionStr    = "2.12.12"
val scalaVersionPrefix = scalaVersionStr.substring(0, 4)

lazy val root = project
  .withId("singlestore-spark-connector")
  .in(file("."))
  .settings(
    name := "singlestore-spark-connector",
    organization := "com.singlestore",
    scalaVersion := scalaVersionStr,
    Compile / unmanagedSourceDirectories += (Compile / sourceDirectory).value / (sparkVersion match {
      case "3.0.0" => "scala-sparkv3.0"
      case "3.1.3" => "scala-sparkv3.1"
      case "3.2.1" => "scala-sparkv3.2"
    }),
    version := s"4.0.0-spark-${sparkVersion}",
    licenses += "Apache-2.0" -> url(
      "http://opensource.org/licenses/Apache-2.0"
    ),
    resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
    libraryDependencies ++= Seq(
      // runtime dependencies
      "org.apache.spark"       %% "spark-core"             % sparkVersion % "provided, test",
      "org.apache.spark"       %% "spark-sql"              % sparkVersion % "provided, test",
      "org.apache.avro"        % "avro"                    % "1.8.2",
      "org.apache.commons"     % "commons-dbcp2"           % "2.7.0",
      "org.scala-lang.modules" %% "scala-java8-compat"     % "0.9.0",
      "com.singlestore"        % "singlestore-jdbc-client" % "1.0.1",
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
    Test / testOptions += Tests.Argument("-oF"),
    Test / fork := true
  )

credentials += Credentials(
  "GnuPG Key ID",
  "gpg",
  "CDD996495CF08BB2041D86D8D1EB3D14F1CD334F",
  "ignored" // this field is ignored; passwords are supplied by pinentry
)

publishTo := sonatypePublishToBundle.value
publishMavenStyle := true
sonatypeSessionName := s"[sbt-sonatype] ${name.value} ${version.value}"
sonatypeProjectHosting := Some(GitHubHosting("memsql", "memsql-spark-connector", "carl@memsql.com"))
