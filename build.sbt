import sbt._
import Keys._

name := "reactive-couch"

organization := "com.geteit"

version := "0.1"

scalaVersion := "2.10.3"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Dependencies.all

resolvers ++= Dependencies.resolvers

parallelExecution in Test := false

lazy val root = (
    Project("reactive-couch", file("."))
    configs(IntegrationTest)
    settings( Defaults.itSettings : _*)
)


