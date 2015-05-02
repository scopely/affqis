name := "affqis"
version := "1.0"
scalaVersion := "2.11.6"

resolvers ++= Seq(
  "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Typesafe Releases" at "http://dl.bintray.com/typesafe/maven-releases/",
  "Jawampa Wamp Stuff" at "https://raw.githubusercontent.com/Matthias247/jawampa/mvn-repo/"
)

libraryDependencies ++= Seq(
  "org.apache.hive" % "hive-jdbc" % "0.13.1-cdh5.3.2" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms"),
    ExclusionRule(name = "slf4j-log4j12")
  ),
  "org.apache.hadoop" % "hadoop-common" % "2.5.0-cdh5.3.2" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms"),
    ExclusionRule(name = "slf4j-log4j12")
  ),

  // Explicit dependency to resolve a warning.
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3",

  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "ws.wamp.jawampa" % "jawampa" % "0.2.0",
  "io.reactivex" %% "rxscala" % "0.24.1"
)
