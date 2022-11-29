import java.nio.file.Files

organization := "fzn.project.bigdata.spark"

name := "elasticsearch-pyspark-30"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.15"

//idePackagePrefix := Some("org.example")

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.4.3" % "provided"
)

Compile / packageBin / mappings := (Compile / packageBin / mappings).value ++
  listPythonFiles(baseDirectory.value / "python")

/**
 * Get list of python files and return the mapping between source files and target paths
 * in the generated package JAR.
 */
def listPythonFiles(pythonBase: File): Seq[(File, String)] = {
  val pythonExcludeDirs = pythonBase / "lib" :: pythonBase / "doc" :: pythonBase / "bin" :: Nil
  import scala.collection.JavaConverters._
  val pythonFiles = Files.walk(pythonBase.toPath).iterator().asScala
    .map { path => path.toFile() }
    .filter { file => file.getName.endsWith(".py") && ! file.getName.contains("test") }
    .filter { file => ! pythonExcludeDirs.exists { base => IO.relativize(base, file).nonEmpty} }
    .toSeq

  pythonFiles pair Path.relativeTo(pythonBase)
}
