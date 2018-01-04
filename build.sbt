name := "hbase-spark-query-language"

organization := "HbaseSparkQL"

version := "0.0.1"

sbtVersion := "0.13.15"

scalaVersion := "2.10.4"

// dep versions
val liftVersion = "2.6+"
val jodaVersion = "2.9.4"
val sparkVersion = "1.6.0"
val hadoopVersion = "2.7.1"
val hbaseVersion = "2.0.0-alpha2"
val hbaseThirdPartyVersion = "1.0.0"
val htraceVersion = "3.1.0-incubating"
// dependencies from maven repos.
libraryDependencies ++= {
  Seq(
    // other
    "net.liftweb"		%%  "lift-json"			% liftVersion,
    "joda-time" 		%  "joda-time" 			% jodaVersion,
    // spark
    "org.apache.spark"  	%% "spark-core"			% sparkVersion	% "provided",
    "org.apache.spark"  	%% "spark-sql"			% sparkVersion  % "provided",
    "org.apache.spark"    %% "spark-mllib"    % sparkVersion  % "provided",
    // hadoop
    // "org.apache.hadoop"   % "hadoop-client"     % hadoopVersion  % "provided",
    // "org.apache.hadoop" 	% "hadoop-common" 		% hadoopVersion  % "provided",
    // "org.apache.hadoop"   % "hadoop-mapreduce-client-core"     % hadoopVersion  % "provided",
    // hbase
    "org.apache.hbase" 		%  "hbase-annotations" 		% hbaseVersion  % "provided",
    "org.apache.hbase" 		%  "hbase-client" 		% hbaseVersion % "provided",
    "org.apache.hbase" 		%  "hbase-common" 		% hbaseVersion % "provided",
    "org.apache.hbase"          %  "hbase-hadoop-compat"        % hbaseVersion % "provided",
    "org.apache.hbase" 		%  "hbase-hadoop2-compat" 	% hbaseVersion  % "provided",
    "org.apache.hbase"          %  "hbase-metrics"              % hbaseVersion  % "provided",
    "org.apache.hbase"          %  "hbase-metrics-api"          % hbaseVersion  % "provided", 
    "org.apache.hbase"		%  "hbase-protocol" 		% hbaseVersion  % "provided",
    "org.apache.hbase"          %  "hbase-protocol-shaded"      % hbaseVersion  % "provided",
    "org.apache.hbase"          %  "hbase-server"		% hbaseVersion % "provided",
    "org.apache.hbase"          %  "hbase-shaded-client"        % hbaseVersion  % "provided",
    "org.apache.hbase"          %  "hbase-shaded-server"        % hbaseVersion  % "provided",
    "org.apache.hbase" 		%  "hbase-spark" 		% hbaseVersion  % "provided",
    "org.apache.hbase.thirdparty" % "hbase-shaded-miscellaneous"  % hbaseThirdPartyVersion,
    "org.apache.hbase.thirdparty" % "hbase-shaded-netty"          % hbaseThirdPartyVersion,
    // htrace
    "org.apache.htrace" 	%  "htrace-core" 		% htraceVersion,
    // utilities
    "org.scalatest" 	    %% "scalatest" 		    % "2.2.6" % "test",
    "com.typesafe"        %  "config"             % "1.2.1",
    "com.google.guava"    %  "guava"              % "11.0.2"
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
