name := "concave_hull"

version := "1.0"

scalaVersion := "2.10.4"


resolvers += "52 North" at "http://52north.org/maven/repo/releases"
resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools"
resolvers += "geotoolkit" at "http://maven.geotoolkit.org/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"
libraryDependencies += "org.postgis" % "postgis-jdbc" % "2.1.3"
libraryDependencies += "org.postgresql" % "postgresql" % "9.4-1201-jdbc41"
libraryDependencies += "com.vividsolutions" % "jts" % "1.13"
libraryDependencies += "org.geotools" % "gt-postgis" % "2.7.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.10.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.0"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first 
}