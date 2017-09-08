name := "MyProject"
version := "0.1"
scalaVersion := "2.11.0"

 
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
   "org.apache.spark" % "spark-streaming-kinesis-asl_2.11" % "2.2.0" % "provided"
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.1.0"


)
 
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}