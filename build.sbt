name := "wikidown-scala"

version := "1.0"

scalaVersion := "2.10.4"

updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.9"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.1" % "provided"

// assembly settings
assemblyJarName in assembly := "wikidown.jar"

test in assembly := {}

mainClass in assembly := Some("WikiDown")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)