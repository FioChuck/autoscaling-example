ThisBuild / organization := "com.example"
ThisBuild / version := "1.0-SNAPSHOT"

// the Scala version that will be used for cross-compiled libraries
ThisBuild / scalaVersion := "2.13.8"

// Workaround for scala-java8-compat issue affecting Lagom dev-mode
// https://github.com/lagom/lagom/issues/3344
ThisBuild / libraryDependencySchemes +=
  "org.scala-lang.modules" %% "scala-java8-compat" % VersionScheme.Always

val macwire = "com.softwaremill.macwire" %% "macros" % "2.3.3" % "provided"
val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1" % Test

lazy val `lagom-scala` = (project in file("."))
  .aggregate(`lagom-scala-api`, `lagom-scala-impl`, `lagom-scala-stream-api`, `lagom-scala-stream-impl`)

lazy val `lagom-scala-api` = (project in file("lagom-scala-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  )

lazy val `lagom-scala-impl` = (project in file("lagom-scala-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslPersistenceCassandra,
      lagomScaladslKafkaBroker,
      lagomScaladslTestKit,
      macwire,
      scalaTest
    )
  )
  .settings(lagomForkedTestSettings)
  .dependsOn(`lagom-scala-api`)

lazy val `lagom-scala-stream-api` = (project in file("lagom-scala-stream-api"))
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslApi
    )
  )

lazy val `lagom-scala-stream-impl` = (project in file("lagom-scala-stream-impl"))
  .enablePlugins(LagomScala)
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslTestKit,
      macwire,
      scalaTest
    )
  )
  .dependsOn(`lagom-scala-stream-api`, `lagom-scala-api`)
