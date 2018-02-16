import NativePackagerHelper._

name := "orac-sdk"

organization := "elegans.io"

version := "0.1"

scalaVersion := "2.11.11"

resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.bintrayRepo("hseeberger", "maven"))

libraryDependencies ++= {
  val elastic_client_version = "6.2.1"
  val spark_version = "2.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % spark_version, 
    "org.apache.spark" %% "spark-mllib" % spark_version,
    "org.elasticsearch" % "elasticsearch-spark-20_2.11" % elastic_client_version,
    "com.github.scopt" %% "scopt" % "3.5.0"
  )
}

scalacOptions += "-deprecation"
scalacOptions += "-feature"

enablePlugins(GitVersioning)
enablePlugins(GitBranchPrompt)
enablePlugins(JavaServerAppPackaging)
enablePlugins(UniversalPlugin)

git.useGitDescribe := true

assemblyMergeStrategy in assembly := {
        case PathList("META-INF", xs @ _*) => MergeStrategy.discard
        case x => MergeStrategy.first
}

