mainClass in (Compile, packageBin) := Some("com.bigchange.basic.DataFrameTest")

name := "AI"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.8" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.4"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-it" % "1.1.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2" % "provided"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.2"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.0.2"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.0.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.2"

libraryDependencies += "redis.clients" % "jedis" % "2.8.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "com.ning" % "async-http-client" % "1.7.16"

libraryDependencies += "net.databinder.dispatch" % "dispatch-core_2.10" % "0.11.0" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.json" % "json" % "20140107" exclude("org.jboss.netty", "netty")

libraryDependencies += "org.jblas" % "jblas" % "1.2.3"

// https://mvnrepository.com/artifact/com.jolbox/bonecp (Lightweight connection pool.）
libraryDependencies += "com.jolbox" % "bonecp" % "0.8.0.RELEASE"

// https://mvnrepository.com/artifact/io.dropwizard.metrics/metrics-core
libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "3.1.2"

// https://mvnrepository.com/artifact/dom4j/dom4j
libraryDependencies += "dom4j" % "dom4j" % "1.6.1"

libraryDependencies += "org.graphstream" % "gs-core" % "1.1.2"

// https://mvnrepository.com/artifact/org.apache.poi/poi-ooxml
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "3.9"

// https://mvnrepository.com/artifact/org.apache.poi/poi
libraryDependencies += "org.apache.poi" % "poi" % "3.9"

// https://mvnrepository.com/artifact/net.sourceforge.jexcelapi/jxl
libraryDependencies += "net.sourceforge.jexcelapi" % "jxl" % "2.6.12"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.9-RC2"


// https://mvnrepository.com/artifact/org.deeplearning4j/dl4j-caffe
// libraryDependencies += "org.deeplearning4j" % "dl4j-caffe" % "0.5.0"

// https://mvnrepository.com/artifact/org.deeplearning4j/deeplearning4j-core
libraryDependencies += "org.deeplearning4j" % "deeplearning4j-core" % "0.6.0"

// https://mvnrepository.com/artifact/org.nd4j/nd4j-native
libraryDependencies += "org.nd4j" % "nd4j-native" % "0.6.0"

libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "0.4.0"

// https://mvnrepository.com/artifact/org.nd4j/nd4j-api
libraryDependencies += "org.nd4j" % "nd4j-api" % "0.6.0"

// https://mvnrepository.com/artifact/org.nd4j/nd4j-jblas
libraryDependencies += "org.nd4j" % "nd4j-jblas" % "0.4-rc3.6"

// https://mvnrepository.com/artifact/org.datavec/datavec-api
libraryDependencies += "org.datavec" % "datavec-api" % "0.6.0"

unmanagedJars in compile += file(local_file_path)

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
    
