addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.0")

addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "0.2.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.8.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.4")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0" excludeAll(ExclusionRule(organization = "com.danieltrinh")))

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.5.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "0.5.1")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-javaversioncheck" % "0.1.0")

libraryDependencies += "org.scalariform" %% "scalariform" % "0.1.7"

resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
