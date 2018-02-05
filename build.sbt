import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "upload",
      scalaVersion := "2.12.3",
      version      := "1.0.0"
    )),
    name := "codeu-upload",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "com.typesafe.akka" %% "akka-stream" % "2.5.9",
      "com.typesafe.akka" %% "akka-http" % "10.0.11"
    ),
  ).enablePlugins(JavaAppPackaging)

