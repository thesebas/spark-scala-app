lazy val root = (project in file(".")).
  settings(
    name := "spark-scala-app",
    version := "1.0",
    scalaVersion := "2.10.4",
    mainClass in Compile := Some("thesebas.spark.MyAppJob")
  )


libraryDependencies ++= Seq(
  "org.json4s" % "json4s-core_2.10" % "3.2.10" exclude("org.slf4j", "slf4j-api"),
  "org.json4s" % "json4s-ast_2.10" % "3.2.10" exclude("org.slf4j", "slf4j-api"),
  "org.json4s" % "json4s-jackson_2.10" % "3.2.10" exclude("org.slf4j", "slf4j-api"),

  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2" exclude("org.slf4j", "slf4j-api"),
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided" exclude("org.slf4j", "slf4j-api"),
  "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided" exclude("org.slf4j", "slf4j-api") //,
  //  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1"
)

//assemblyJarName in assembly := "MyApp-fat.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}