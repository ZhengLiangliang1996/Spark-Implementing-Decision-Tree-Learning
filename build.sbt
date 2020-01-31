name := "BigData_1stWPO"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0"

mainClass in Compile := Some("BigDataProject")

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.example",
  scalaVersion := "2.11.12",
  test in assembly := {}
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("BigDataProject")
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _ *) => MergeStrategy.discard

  case x =>
    MergeStrategy.first
}

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "project.jar"

/* you need to be able to undo the "provided" annotation on the deps when running your spark
   programs locally i.e. from sbt; this bit reincludes the full classpaths in the compile and run tasks. */
fullClasspath in Runtime := (fullClasspath in (Compile, run)).value