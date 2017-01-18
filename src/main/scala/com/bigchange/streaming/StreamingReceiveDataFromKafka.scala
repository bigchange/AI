package com.bigchange.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by C.J.YOU on 2016/12/5.
  */
object StreamingReceiveDataFromKafka {

  def showWarning(args: Array[String]): Unit ={
    if (args.length < 3) {
      System.err.println(
        """
          |Usage: ReceiveDataFromKafka <brokers> <topics> <zkhosts> <checkpoint_dir> <groupId>
          |<brokers> is a list of one or more Kafka brokers
          |<topics> is a list of one or more kafka topics to consume from
          |<zkhosts> is a list of zookeeper to consume from
        """.stripMargin)
      System.exit(1)
    }
  }


  def main(args: Array[String]) {

    showWarning(args)

    val sparkConf = new SparkConf()
      .setAppName("ReceiveDataFromKafka")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.cleaner.ttl", "10000")
      .set("spark.akka.frameSize","256")
    // .setMaster("local")

    System.setProperty("spark.scheduler.mode", "FAIR")  // 公平调度

    val Array(brokers, topics, zkhosts, checkpointDir, groupId) = args

    def createStreamingContext: () => StreamingContext = {

      val ssc = new StreamingContext(sparkConf, Seconds(60))

      // NOTE: You have to create dstreams inside the method
      // See http://stackoverflow.com/q/35090180/1305344

      val lineData = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
        ssc,
        kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> groupId,"zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),
        topics = topics.split(",").toSet
      )

      val result = lineData.flatMap(x => x._2)

      // data process procedure
      // .....

      ssc.checkpoint(checkpointDir)
      () => ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)

    val sc = ssc.sparkContext

    /*val numStream = 3
    val streamNmuber = (1 to numStream).map{ i => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "Telecom","zookeeper.connect" -> zkhosts),
      topics = topics.split(",").toSet
    ) }
    val lineData = ssc.union(streamNmuber)*/

    // Start streaming processing
    ssc.start
    ssc.awaitTermination()

  }


}
