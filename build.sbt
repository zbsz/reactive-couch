import sbt._
import Keys._

name := "reactive-couch"

organization := "com.geteit"

version := "0.2-SNAPSHOT"

scalaVersion := "2.10.3"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Dependencies.all

resolvers ++= Dependencies.resolvers

publishTo := {
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("snapshots", new File("../mvn-repo/snapshots" )) )
  else
    Some(Resolver.file("releases", new File("../mvn-repo/releases" )) )
}

parallelExecution in Test := false

parallelExecution in IntegrationTest := false

lazy val root = (
    Project("reactive-couch", file("."))
    configs IntegrationTest
    settings( Defaults.itSettings : _*)
)


