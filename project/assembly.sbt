addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0" % "provided"
)

