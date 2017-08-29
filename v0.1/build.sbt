name := "sparkql"

version := "1.0"

scalaVersion := "2.10.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.1"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
