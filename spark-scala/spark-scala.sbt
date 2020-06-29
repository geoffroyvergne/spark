name := "Spark Scala"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion: String = "2.1.1"
val sparkCsvVersion: String = "1.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

libraryDependencies += "com.databricks" %% "spark-csv" % sparkCsvVersion

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
//libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"

//libraryDependencies += "org.restlet.jee" % "org.restlet" % "2.3.0"
//libraryDependencies += "com.lucidworks.spark" % "spark-solr" % "3.0.2"

//libraryDependencies += "com.lucidworks.spark" % "spark-solr" % "3.1.1"

//libraryDependencies += "org.apache.solr" % "solr-core" % "6.6.0"

