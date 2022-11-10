name := "st4ml_examples"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.+" % "provided"

libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.18.2"

libraryDependencies += "org.locationtech.jts.io" % "jts-io-common" % "1.18.2"

lazy val st4ml = RootProject(file("../st4ml"))

lazy val root = project in file(".") dependsOn(st4ml)

exportJars := true

resolvers += Resolver.url("maven_central", url("https://repo.maven.apache.org/maven2/"))
