val commonSettings = Seq(
  version := "0.0.1",
  scalaVersion := "2.12.8",
  libraryDependencies ++= Seq(
    "com.google.cloud" % "google-cloud-pubsub" % "1.55.0",
    "org.apache.commons" % "commons-text" % "1.6",
    "com.typesafe.akka" %% "akka-stream" % "2.5.17",
    "org.slf4j" % "slf4j-api" % "1.7.25",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "ch.qos.logback" % "logback-core" % "1.2.3",
    "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
)

val gcp = project.in(file("gcp"))
  .settings(commonSettings)
