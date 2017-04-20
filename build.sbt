name := "spark-xml"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.5.2",
  "org.apache.hadoop" % "hadoop-streaming" % "2.7.1",
  "org.apache.hadoop" % "hadoop-common" % "2.7.1",
  "org.json" % "json" % "20151123",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "org.apache.spark" % "spark-sql_2.11" % "1.5.2"
)
