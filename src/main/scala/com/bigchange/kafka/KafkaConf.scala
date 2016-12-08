package com.bigchange.kafka

/*
 kafka 配置实现类
 */
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils


object KafkaConf {

   val zkQuorum = "61.147.114.81:2181,61.147.114.82:2181,61.147.114.84:2181,61.147.114.80:2181,61.147.114.85:2181"

   //val group = "Spark_" //需要确保每个提交的job的kafka group名称不同
   val topics = "yangdecheng"
   val numThreads = 2

  def createStream(
                    ssc: StreamingContext,
                    zkQuorum: String,
                    groupId: String,
                    topics: Map[String, Int],
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
                    ): ReceiverInputDStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> groupId,
      "zookeeper.session.timeout.ms" -> "68000",
      "zookeeper.connection.timeout.ms" -> "105000",
      "zookeeper.sync.time.ms" -> "12000",
      "rebalance.max.retries"->"6",
      "rebalance.backoff.ms"->"9800",
      "auto.offset.reset" -> "largest")
    KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, storageLevel)
  }

  def getStreaming(ssc: StreamingContext, groupId: String)  = {
     createStream(ssc, zkQuorum, groupId, Map(topics -> 1))
  }

}
