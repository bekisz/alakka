name := "alakka"

version := "1.0"

scalaVersion := "2.12.12"

lazy val akkaVersion = "2.6.10"

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/releases"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  //"ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  //"org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.0",
  "com.storm-enroute" %% "scalameter-core" % "0.19" ,
  "org.apache.spark" %% "spark-sql" % "2.4.7" ,
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
)



