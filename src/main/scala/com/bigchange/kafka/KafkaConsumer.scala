package com.bigchange.kafka

import com.bigchange.config.Parameter
import com.bigchange.util.{FileUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}


/*
 * kafka消费者
 * 如果需要另保存消费的offset 可以通过如下方式
 *   KafkaUtils.createDirectStream(...).foreachRDD { rdd =>
 *      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
 *      ...
 *   }
 */
class KafkaConsumer(parameter: Parameter) {

  private val zkQuorum = parameter.getParameterByTagName("kafka.zkQuorum")

  private  val numThreads = parameter.getParameterByTagName("kafka.numThreads")

  private  val topics = parameter.getParameterByTagName("kafka.consumerTopic").split(",")

  private  val groupId = parameter.getParameterByTagName("kafka.groupId")

  private val brokers = parameter.getParameterByTagName("kafka.brokerList")

  private  val offsetLog = parameter.getParameterByTagName("kafka.offset.log")

  /**
    * 运行在Spark workers/executors上的Kafka Receivers连续不断地从Kafka中读取数据，其中用到了Kafka中高层次的消费者API。
    * 接收到的数据被存储在Spark workers/executors中的内存，同时也被写入到WAL中。只有接收到的数据被持久化到log中，
    * Kafka Receivers才会去更新Zookeeper中Kafka的偏移量。
    * 接收到的数据和WAL存储位置信息被可靠地存储，如果期间出现故障，这些信息被用来从错误中恢复，并继续处理数据。
    * 这个方法可以保证从Kafka接收的数据不被丢失。但是在失败的情况下，
    * 有些数据很有可能会被处理不止一次！这种情况在一些接收到的数据被可靠地保存到WAL中，
    * 但是还没有来得及更新Zookeeper中Kafka偏移量，系统出现故障的情况下发生。
    * 这导致数据出现不一致性：Spark Streaming知道数据被接收，但是Kafka那边认为数据还没有被接收，
    * 这样在系统恢复正常时，Kafka会再一次发送这些数据
 *
    * @param ssc  ssc
    * @param zkQuorum Zookeeper quorum (hostname:port,hostname:port,..)
    * @param groupId groupId
    * @param topics  Map of (topic_name -> numPartitions) to consume. Each partition is consumed
    *                    in its own thread.
    * @param storageLevel  storageLevel
    * @return  DStream of (Kafka message key, Kafka message value)
    */
  private def createStream(
                    ssc: StreamingContext,
                    zkQuorum: String,
                    groupId: String,
                    topics: Map[String, Int],
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
                  ): ReceiverInputDStream[(String, String)] = {

    // 0.9.0.0  a replacement for the older Scala-based simple and high-level consumers -> New Consumer Configs
    //  Old Consumer Configs
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

  /**
    * 它不是启动一个Receivers来连续不断地从Kafka中接收数据并写入到WAL中，
    * 而且简单地给出每个batch区间需要读取的偏移量位置，
    * 最后，每个batch的Job被运行，那些对应偏移量的数据在Kafka中已经准备好了。
    * 这些偏移量信息也被可靠地存储（checkpoint），
    * 在从失败中恢复可以直接读取这些偏移量信息。
 *
    * @param ssc  ssc
    * @param brokers brokers
    * @param zkQuorum Zookeeper quorum (hostname:port,hostname:port,..)
    * @param groupId groupId
    * @param topics  Map of (topic_name -> numPartitions) to consume. Each partition is consumed
    *                    in its own thread.
    * @param storageLevel  storageLevel
    * @return  DStream of (Kafka message key, Kafka message value)
    */
  private def createDirectStream(
                          ssc: StreamingContext,
                          brokers: String,
                          zkQuorum: String,
                          groupId: String,
                          topics:Set[String],
                          storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER): InputDStream[(String, String)] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String](
        "metadata.broker.list" -> brokers,
        "group.id" -> groupId,
        "zookeeper.connect" -> zkQuorum,
        "serializer.class" -> "kafka.serializer.StringEncoder"),
      topics
    )

  }

  def getStreaming(ssc: StreamingContext)  = createStream(ssc, zkQuorum, groupId, topics.map((_, numThreads.toInt)).toMap)

  def getDirectStreaming(ssc: StreamingContext, checkPointDir: String) = {

    ssc.checkpoint(checkPointDir)
    createDirectStream(ssc, brokers, zkQuorum, groupId, topics.toSet)

  }

  /**
    * 记录消费的offset for DirectStreaming Api
    * 方便在Driver失败后，对比是否有数据没有消费到
 *
    * @param rdd Streaming RDD
    */
  def dealWithOffset(rdd: RDD[(String, String)]) = {

    val offSetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    val ts = System.currentTimeMillis()

    val log = offSetRanges.map(x => ts + " =====> topic:"+ x.topic + "\t partition:" + x.partition + "\t fromOffset:" + x.fromOffset + "\t untilOffset:" + x.untilOffset )

    FileUtil.saveData(offsetLog, log)

  }

}

object KafkaConsumer {

  private var kc: KafkaConsumer = null

  def apply(parameter: Parameter): KafkaConsumer = {

    if(kc == null) kc = new KafkaConsumer(parameter)

    kc

  }

  def getInstance = kc


}
