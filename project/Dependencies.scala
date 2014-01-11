import sbt._
import Keys._

object Dependencies {

  val local = "Local Maven repo" at "file://"+Path.userHome+"/.m2/repository"
  val typesafe = "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
  val scalaTools = "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

  val snapshots = "oss-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
  val releases = "oss-releases"  at "http://oss.sonatype.org/content/repositories/releases"
  val sprayRepo = "spray repo" at "http://repo.spray.io"

  val resolvers = Seq(local, typesafe, releases, snapshots, scalaTools, sprayRepo)

  val akkaVersion = "2.2.3"

  val akka = "com.typesafe.akka" %% "akka-actor" % akkaVersion
  val testkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion

  val logback = "ch.qos.logback" % "logback-classic" % "1.0.13"
  val jmemcached = "com.thimbleware.jmemcached" % "jmemcached-core" % "1.0.0"


  val iteratees = "com.typesafe.play" %% "play-iteratees" % "2.2.0"
  val playJson = "com.typesafe.play" %% "play-json" % "2.2.0"
  val parboiled = "org.parboiled" %% "parboiled-scala" % "1.1.6"

  val sprayClient = "io.spray" % "spray-client" % "1.2-RC2"
  val sprayCan = "io.spray" % "spray-can" % "1.2-RC2"
  val specs = "org.specs2" %% "specs2" % "2.2.3"
  val scalatest = "org.scalatest" %% "scalatest" % "2.0.RC1"
  val scalacheck = "org.scalacheck" %% "scalacheck" % "1.10.1"

  val all = Seq(akka, sprayCan, sprayClient, iteratees, parboiled, playJson, scalacheck % "test", scalatest % "it,test", jmemcached % "it,test", testkit % "it,test", logback % "it,test")
}
