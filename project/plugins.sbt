addSbtPlugin("org.xerial.sbt" % "sbt-sonatype"  % "3.9.6")
addSbtPlugin("com.jsuereth"   % "sbt-pgp"       % "2.0.1")
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo" % "0.11.0")
addSbtPlugin("com.eed3si9n"   % "sbt-assembly"  % "2.2.0")
// Custom version of fm-sbt-s3-resolver published to our Artifactory for DEVX-275
// See repo here: https://github.com/ActionIQ/sbt-s3-resolver
addSbtPlugin("co.actioniq" % "sbt-s3-resolver" % "1.0.1")
