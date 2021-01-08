name := "alakka"
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.12"
scalacOptions += "-target:jvm-1.8"



lazy val akkaVersion = "2.5.21"
lazy val sparkVersion = "2.3.2"


credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

ThisBuild / organization := "com.github.bekisz"
ThisBuild / organizationName := "example"
ThisBuild / organizationHomepage := Some(url("http://example.com/"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/bekisz/alakka"),
    "scm:git@github.com:bekisz/alakka.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "szabi",
    name  = "Szabolcs Beki",
    email = "szabi@apache.org",
    url   = url("https://github.com/bekisz")
  )
)

ThisBuild / description := "Artificial Life Framework"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/example/project"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true


//Compile / resourceDirectory := baseDirectory.value / "config"
//Compile / unmanagedResourceDirectories += baseDirectory.value / "config"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  //"ch.qos.logback" % "logback-classic" % "1.2.3",
  //"com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test % "provided",
   "org.scalatest" %% "scalatest-funsuite" % "3.2.3" % Test,
  "org.scalatest" %% "scalatest" % "3.2.3" % Test,

  "org.apache.spark" %% "spark-sql" %  sparkVersion,
   "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"

)

/*
fork := true
javaOptions ++= Seq(
  "-D-Dspark.master=local[*]"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard

  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList("javax", "transaction", xs @ _*)     => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*)     => MergeStrategy.first
  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first

  case x => MergeStrategy.first
}
*/