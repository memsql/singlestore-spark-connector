addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.0")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.3")

addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "0.2.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.8.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.4")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0" excludeAll(ExclusionRule(organization = "com.danieltrinh")))

addSbtPlugin("ohnosequences" % "sbt-s3-resolver" % "0.12.0")

libraryDependencies += "org.scalariform" %% "scalariform" % "0.1.7"

resolvers += "Era7 maven releases" at "https://s3-eu-west-1.amazonaws.com/releases.era7.com"

resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
