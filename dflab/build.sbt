lazy val root = (project in file("."))
  .settings(
      name := "trianglesDF",
      version := "1.0",
      scalaVersion := "2.12.15"
   )
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"
)
