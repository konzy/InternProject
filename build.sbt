name := "TestScalaSpark"

version := "1.0"

scalaVersion := "2.11.11"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.1.1" % "provided",

  "org.apache.spark" % "spark-sql_2.11" % "2.1.1")

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.1"

libraryDependencies += "com.databricks" % "spark-xml_2.11" % "0.4.1"
