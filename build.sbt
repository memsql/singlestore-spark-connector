import xerial.sbt.Sonatype._

/*
  To run tests or publish with a specific spark version use this java option:
    -Dspark.version=2.3.4
 */
val sparkVersion = sys.props.get("spark.version").getOrElse("2.4.4")

/*
  To run tests with a specific mysql driver use this java option:
    -Dmysql.driver=mysql5
 */

val mysqlDriver = sys.props.getOrElse("mysql.driver", "mariadb") match {
  case "mariadb" => "org.mariadb.jdbc" % "mariadb-java-client"  % "2.+"
  case "mysql5"  => "mysql"            % "mysql-connector-java" % "5.+"
  case "mysql8"  => "mysql"            % "mysql-connector-java" % "8.+"
  case _ =>
    throw new IllegalArgumentException(
      "java option mysql.driver should be one of { mariadb, mysql5, mysql8 }"
    )
}

lazy val root = project
  .withId("memsql-spark-connector")
  .in(file("."))
  .settings(
    name := "memsql-spark-connector",
    organization := "com.memsql",
    scalaVersion := "2.11.11",
    version := s"3.0.0-beta-spark-${sparkVersion}",
    licenses += "Apache-2.0" -> url(
      "http://opensource.org/licenses/Apache-2.0"
    ),
    resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
    libraryDependencies ++= Seq(
      // runtime dependencies
      "org.apache.spark"       %% "spark-core"             % sparkVersion % "provided, test",
      "org.apache.spark"       %% "spark-sql"              % sparkVersion % "provided, test",
      "org.apache.commons"     % "commons-dbcp2"           % "2.7.0",
      "org.scala-lang.modules" % "scala-java8-compat_2.11" % "0.9.0",
      // test dependencies
      "org.scalatest"  %% "scalatest" % "3.1.0" % Test,
      "org.scalacheck" %% "scalacheck" % "1.14.1" % Test,
      "mrpowers"       % "spark-daria" % "0.35.0-s_2.11" % Test,
      "MrPowers"       % "spark-fast-tests" % "0.20.0-s_2.11" % Test,
      mysqlDriver      % Test
    ),
    Test / testOptions += Tests.Argument("-oF"),
    Test / fork := true,
    //Test / javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-Dmemsql.invariant=true"),
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
