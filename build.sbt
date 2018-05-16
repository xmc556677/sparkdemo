name := "sparkdemo"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/content/repositories/releases/"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion  % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVersion,
  "org.apache.hadoop" % "hadoop-common" % "2.7.4" ,//% Provided,
  "org.apache.hbase" % "hbase-server" % "1.3.1",
  "org.apache.hbase" % "hbase-common" % "1.3.1" ,
  "org.apache.hbase" % "hbase-client" % "1.3.1",
  "eu.unicredit" %% "hbase-rdd" % "0.8.0",
  "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3",
  "org.scalanlp" %% "breeze" % "0.13.2" % Provided,
  "org.scalanlp" %% "breeze-natives" % "0.13.2",
  "org.scodec" %% "scodec-core" % "1.10.3",
  "org.scodec" %% "scodec-protocols" % "1.0.2",
  "org.scodec" %% "scodec-stream" % "1.0.1",
  "org.pcap4j" % "pcap4j-core" % "1.7.2",
  "org.pcap4j" % "pcap4j-packetfactory-static" % "1.7.2",
  "io.pkts" % "pkts-core" % "3.0.2",
  "org.msgpack" %% "msgpack-scala" % "0.8.13",
  "org.msgpack" % "jackson-dataformat-msgpack" % "0.8.13",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.first
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { _.data.getName().toString() match {
    case "commons-io-2.4.jar" => true
    case "jetty-util-6.1.26.jar" => true
    case "guava-12.0.1.jar" => true
    case _ => false
  }}
}

packExcludeJars := Seq(
//  "scala-.*\\.jar",
  "commons-io-.*\\.jar",
  "jetty-util-.*\\.jar",
  "guava-.*\\.jar"
)

//lazy val karken = ProjectRef(uri("https://github.com/OpenSOC/kraken.git"), "kraken")

//lazy val root = (project in file(".")).dependsOn(karken)

