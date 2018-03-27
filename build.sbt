import NativePackagerHelper._
import sbt.url

name := "orac-sdk"

organization := "elegans.io"

version := "0.1"

scalaVersion := "2.11.11"
crossScalaVersions := Seq("2.11.11")

resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.bintrayRepo("hseeberger", "maven"))

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers +=
  "Sonatype OSS Releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"

libraryDependencies ++= {
  val AkkaVersion       = "2.5.8"
  val AkkaHttpVersion   = "10.1.0"
  val ElasticClientVersion = "6.2.1"
  val SparkVersion = "2.2.1"
  val OracEntitiesVersion = "1.0.0"
  Seq(
    "com.github.scopt" %% "scopt" % "3.5.0",
    "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
    "com.typesafe.akka" %% "akka-contrib" % AkkaVersion,
    "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-core" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % AkkaVersion,
    "com.typesafe.akka" %% "akka-typed" % AkkaVersion,
    "io.elegans" %% "orac-entities" % OracEntitiesVersion,
    "org.apache.spark" %% "spark-core" % SparkVersion,
    "org.apache.spark" %% "spark-mllib" % SparkVersion
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

enablePlugins(GitBranchPrompt)
enablePlugins(GitVersioning)
enablePlugins(UniversalPlugin)

git.useGitDescribe := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

homepage := Some(url("http://www.elegans.io"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/elegans-io/orac-sdk"),
    "scm:git@github.com:elegans-io/orac-sdk.git"
  )
)

developers := List(
  Developer(
    id    = "angleto",
    name  = "Angelo Leto",
    email = "angelo.leto@elegans.io",
    url   = url("http://www.elegans.io")
  )
)

