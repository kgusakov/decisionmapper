name := "Decision Mapper"

version := "1.0"

scalaVersion := "2.12.10"

scalafmtOnCompile := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "io.argonaut" %% "argonaut" % "6.2.2"
)

mainClass in assembly := Some("com.exam.decisionmapper.DecisionMapperApp")
assemblyJarName in assembly := "decisionmapper-assembly.jar"