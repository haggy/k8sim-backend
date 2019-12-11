name := "subsystem"

version := "0.1"

scalaVersion := "2.13.1"

val versions = Map(
  'akka -> "2.6.1",
  'logback -> "1.2.3"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % versions('akka),
  "ch.qos.logback" % "logback-classic" % versions('logback)
)