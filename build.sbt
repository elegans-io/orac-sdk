name := "orac-sdk"

version := "0.1"

scalaVersion := "2.12.4"

val sparkVersion = "2.2.1"

organization := "io.elegans"

resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.bintrayRepo("hseeberger", "maven"))

libraryDependencies ++= {
  val elastic_client_version = "5.4.2"
  val spark_version = "2.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % spark_version % "provided",
    "org.apache.spark" %% "spark-mllib" % spark_version % "provided",
    "org.elasticsearch" % "elasticsearch-spark-20_2.11" % elastic_client_version,
  )
}
