name := "hbase-spark-query-language"

organization := "HbaseSparkQL"

version := "0.0.1"

sbtVersion := "0.13.15"

scalaVersion := "2.11.8"

// dep versions
val liftVersion = "2.6+"
val jodaVersion = "2.9.4"
val sparkVersion = "2.1.0"
val hadoopVersion = "2.7.1"
val hbaseVersion = "1.2.3"
val htraceVersion = "3.1.0-incubating"
// external jars
val hbaseSparkJarLocation = "Please Include the Location of the Hbase-Spark Module Jar on your Disk!"
// dependencies from maven repos.
libraryDependencies ++= {
  Seq(
    // general
    "net.liftweb"		%%  "lift-json"			% liftVersion,
    "joda-time" 		%  "joda-time" 			% jodaVersion,
    // spark
    "org.apache.spark"  	%% "spark-core"			% sparkVersion	% "provided",
    "org.apache.spark"  	%% "spark-sql"			% sparkVersion,
    "org.apache.spark"		%% "spark-mllib"		% sparkVersion,
    "org.apache.spark" 		%% "spark-streaming" 		% sparkVersion,
    "org.apache.spark"		% "spark-streaming-kafka-0-8_2.11" 	% sparkVersion,
    // hbase
    "org.apache.hbase" 		%  "hbase-annotations" 		% hbaseVersion,
    "org.apache.hbase" 		%  "hbase-client" 		% hbaseVersion,
    "org.apache.hbase" 		%  "hbase-common" 		% hbaseVersion,
    "org.apache.hbase" 		%  "hbase-hadoop-compat" 	% hbaseVersion,
    "org.apache.hbase"		%  "hbase-protocol" 		% hbaseVersion,
    "org.apache.hbase" 		%  "hbase-server" 		% hbaseVersion,
    "org.apache.hbase" 		%  "hbase-spark" 		% hbaseVersion	from hbaseSparkJarLocation,
    // htrace
    "org.apache.htrace" 	%  "htrace-core" 		% htraceVersion,
    // utilities
    "org.scalatest" 	    %% "scalatest" 		    % "2.2.6" % "test",
    "com.typesafe"          %  "config"             % "1.2.1"
  )
}

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Apache Releases" at "https://repository.apache.org/content/repositories/releases",
  "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  "Spark Artifacts Repository" at "https://mvnrepository.com/artifact/org.apache.spark/",
  "Databricks Repository" at "https://mvnrepository.com/artifact/databricks/",
  Resolver.sonatypeRepo("public")
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}

initialize := {
    val _ = initialize.value // run the previous initialization
    val required = "1.8"
    val current  = sys.props("java.specification.version")
    assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
}
