name := "scala_spark"

version := "0.1"



scalaVersion := "2.12.8"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "net.liftweb" %% "lift-json" % "3.4.0"
)
