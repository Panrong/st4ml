name := "st-tool"

version := "3.0"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.+" % "provided"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.+" % "provided"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"

libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.17.1"