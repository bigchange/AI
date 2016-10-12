mainClass in (Compile, packageBin) := Some("com.bigchange.basic.DataFrameTest")
name := "AI"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.4" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.4"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-it" % "1.1.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.5.2" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.2" exclude("org.jboss.netty", "netty")

libraryDependencies += "redis.clients" % "jedis" % "2.8.0"

libraryDependencies += "com.ning" % "async-http-client" % "1.7.16"

libraryDependencies += "net.databinder.dispatch" % "dispatch-core_2.10" % "0.11.0" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.json" % "json" % "20140107" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.jblas" % "jblas" % "1.2.3"

// https://mvnrepository.com/artifact/org.deeplearning4j/dl4j-caffe
libraryDependencies += "org.deeplearning4j" % "dl4j-caffe" % "0.5.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "3.0.0" exclude("org.jboss.netty", "netty")

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
    
