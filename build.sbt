

name := "alakka"

version := "1.0"

scalaVersion := "2.11.12"
//scalaVersion := "2.12.12"
//scalaVersion := "2.13.4"

//assemblyJarName:= "baeldung-scala-sbt-assembly-fatjar-1.0.jar"

//lazy val akkaVersion = "2.6.10"
lazy val akkaVersion = "2.5.21"
lazy val sparkVersion = "2.3.2"

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/releases"

//Compile / resourceDirectory := baseDirectory.value / "config"
//Compile / unmanagedResourceDirectories += baseDirectory.value / "config"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  //"ch.qos.logback" % "logback-classic" % "1.2.3",
  //"com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test % "provided",
  //"org.scalatest" %% "scalatest" % "3.1.0" % Test,
  //"org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.0",
  "com.storm-enroute" %% "scalameter-core" % "0.19",
   "org.apache.spark" %% "spark-sql" %  sparkVersion  /* %  "provided" */,
   "org.apache.spark" %% "spark-core" % sparkVersion /* %  "provided" */,
  //"org.apache.spark" %% "spark-streaming" % "2.4.7" % "provided",

  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
  //"com.cloudera.livy" %% "livy-scala-api" % "0.3.0"
  //"org.apache.livy" %% "livy-scala-api" % "0.5.0-incubating" % "provided",
  //"org.apache.livy" %% "livy-core" % "0.5.0-incubating" % "provided",
  //"org.apache.livy" %% "livy-repl" % "0.5.0-incubating"% "provided"

)

/*
fork := true
javaOptions ++= Seq(
  "-D-Dspark.master=local[*]"
) */
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard

  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList("javax", "transaction", xs @ _*)     => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*)     => MergeStrategy.first
  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first

  case x => MergeStrategy.first
}
