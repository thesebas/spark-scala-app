lazy val root = (project in file(".")).
  settings(
    name := "spark-scala-app",
    version := "1.0",
    scalaVersion := "2.10.4",
    mainClass in Compile := Some("thesebas.spark.MyAppJob")
  )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"//,
//  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1"
)

//assemblyJarName in assembly := "MyApp-fat.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}