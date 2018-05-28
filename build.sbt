name := "data61paradise"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"
val mysqlversion = "5.1.12"
val configversion = "1.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "mysql" % "mysql-connector-java" % mysqlversion,
  "com.typesafe" % "config" % configversion
)