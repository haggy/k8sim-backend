name := "subsystem"

version := "0.1"

scalaVersion := "2.12.8"

val versions = Map(
  'akka -> "2.6.1",
  'logback -> "1.2.3",
  'circe -> "0.11.1",
  'finch -> "0.31.0"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % versions('akka),
  "ch.qos.logback" % "logback-classic" % versions('logback),
  "com.github.finagle" %% "finch-core" % versions('finch),
  "com.github.finagle" %% "finch-circe" % versions('finch),
  "io.circe" %% "circe-generic" % versions('circe)
)