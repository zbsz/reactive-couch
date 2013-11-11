import sbt._
import Keys._

object Dependencies {

  val local = "Local Maven repo" at "file://"+Path.userHome+"/.m2/repository"
  val typesafe = "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
  val scalaTools = "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

  val snapshots = "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
  val releases = "releases"  at "http://oss.sonatype.org/content/repositories/releases"
  val sprayRepo = "spray repo" at "http://repo.spray.io"

  val resolvers = Seq(local, typesafe, releases, snapshots, scalaTools, sprayRepo)

  val akkaVersion = "2.2.3"

  val akka = "com.typesafe.akka" %% "akka-actor" % akkaVersion
  val testkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
  val guava = "com.google.guava" % "guava" % "15.0"

  val logback = "ch.qos.logback" % "logback-classic" % "1.0.13"
  val jmemcached = "com.thimbleware.jmemcached" % "jmemcached-core" % "1.0.0"

  val sprayClient = "io.spray" % "spray-client" % "1.2-RC2"
  val sprayCan = "io.spray" % "spray-can" % "1.2-RC2"
  val sprayJson = "io.spray" %% "spray-json" % "1.2.5"
  val gson = "com.google.code.gson" % "gson" % "1.7.1"
  val specs = "org.specs2" %% "specs2" % "2.2.3"
  val scalatest = "org.scalatest" %% "scalatest" % "2.0.RC1"
  val scalacheck = "org.scalacheck" %% "scalacheck" % "1.10.1"

  val all = Seq(akka, guava, sprayCan, sprayClient, sprayJson, gson, scalacheck % "test", scalatest % "it,test", jmemcached % "it,test", testkit % "it,test", logback % "it,test")
}
