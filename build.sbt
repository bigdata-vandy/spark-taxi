organization := "edu.vanderbilt.accre"

version := "0.1.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "latest.integration" % "test",
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0"
)

