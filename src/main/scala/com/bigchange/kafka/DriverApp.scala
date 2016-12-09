package com.bigchange.kafka

import java.text.SimpleDateFormat
import java.util.Date

import com.bigchange.config.Dom4jParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DriverApp {

  def send2Kafka(key:String, value: String) = KafkaProducer.getInstance.send("send message ....")

  def send2Kafka2(key:String, value: String) = KafkaProducer.getInstance.send("send message ...")

  val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")

  def getTime: String = format.format(new Date())

  //get time : 201504071722, 年月日时分秒
  def get_curr_time: String = format.format(new Date())

  var sequence_number = -1
  var last_modified_time = System.currentTimeMillis
  var skey_prefix = get_curr_time

  def main(args: Array[String]): Unit = {

    // System.setProperty("spark.default.parallelism", "150")
    // System.setProperty("spark.akka.frameSize", "30")
    // System.setProperty("spark.network.timeout", "1200")
    // System.setProperty("spark.task.cpus", "5")  spark.shuffle.memoryFraction
    // System.setProperty("spark.storage.memoryFraction", "0.7")
    // System.setProperty("spark.akka.threads", "16")
    // System.setProperty("spark.serializer", "org.apache.spark.serializer.KeyoSerializer")
    // System.setProperty("spark.shuffle.memoryFraction", "0")
    // System.setProperty("spark.yarn.executor.memoryOverhead", "5120")
    // System.setProperty("spark.yarn.driver.memoryOverhead", "5120")

    System.setProperty("spark.shuffle.consolidateFiles", "true")
    System.setProperty("spark.speculation", "true")
    System.setProperty("spark.streaming.concurrentJobs", "10")
    System.setProperty("spark.yarn.max.executor.failures", "99")
    System.setProperty("spark.streaming.blockInterval", "100")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.rpc.numRetries", "6")
    System.setProperty("spark.executor.heartbeatInterval", "20")
    System.setProperty("spark.task.maxFailures", "8")
    System.setProperty("spark.network.timeout", "1000")

    if (args.length < 0) {
      System.err.println("Usage: <xmlFile>")
      System.exit(1)
    }


   //  val Array(xmlFile) = args
    val xmlFile = "src/test/resources/config.xml"
    val parameter = Dom4jParser.apply(xmlFile)

    KafkaConsumer.apply(parameter)
    KafkaProducer.apply(parameter)

    val ssc = new StreamingContext(new SparkConf().setAppName("spark.appName").setMaster(parameter.getParameterByTagName("spark.master")), Seconds(60))

    // kafka 通过定义多个接受器实现数据的高效并发接收，但需要注意 每个接受器需要有一个c至少一个cpu核支持，所以资源充足的时候可以考虑使用
    val numStrams = 5

    val kafkaStreams=(1 to numStrams).map{ i => KafkaConsumer.getInstance.getStreaming(ssc).map(_._2) }

//   val ts: String = getTime
//   val linesdata=ssc.union(kafkaStreams).saveAsTextFiles("hdfs://ns1/user/hadoop/kunyan/"+ts+"/")

    val lineData=ssc.union(kafkaStreams)

    lineData.foreachRDD { rdd =>

      rdd.foreach(println)

    }

    ssc.start()
    ssc.awaitTermination()

  }
}
