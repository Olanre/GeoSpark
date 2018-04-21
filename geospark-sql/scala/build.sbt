import sbt.Keys.{libraryDependencies, version}


lazy val root = (project in file(".")).
  settings(
    name := "SpatialBenchMarks",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization := "org.datasyslab",

    publishMavenStyle := true
  )

val SparkVersion = "2.2.1"

val SparkCompatibleVersion = "2.2"

val HadoopVersion = "2.7.2"

val GeoSparkVersion = "1.1.2"

val dependencyScope = "compile"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope,
  "org.datasyslab" % "geospark" % GeoSparkVersion,
  "org.datasyslab" % "geospark-sql_".concat(SparkCompatibleVersion) % GeoSparkVersion ,
  "org.datasyslab" % "geospark-viz" % GeoSparkVersion,
  "commons-io" % "commons-io" % "2.4",
  "com.google.guava" % "guava" % "14.0.1" % "provided",
  "org.slf4j" % "slf4j-api" % "1.7.16" % "provided",
  "com.lihaoyi" % "fastparse_2.11" % "0.4.3" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.vividsolutions" % "jts" % "1.13" % "test",
  "com.esri.geometry" % "esri-geometry-api" % "1.2.1"
)

// https://mvnrepository.com/artifact/harsha2010/magellan
libraryDependencies += "harsha2010" % "magellan" % "1.0.5-s_2.11"

//unmanagedJars in Compile += file("lib/magellan.jar")


assemblyMergeStrategy in assembly := {
  case PathList("org.datasyslab", "geospark", xs@_*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case path if path.endsWith(".SF") => MergeStrategy.discard
  case path if path.endsWith(".DSA") => MergeStrategy.discard
  case path if path.endsWith(".RSA") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"

resolvers += "Maven repository" at "https://dl.bintray.com/spark-packages/maven/"