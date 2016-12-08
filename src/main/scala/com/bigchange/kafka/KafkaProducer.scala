package com.bigchange.kafka

/*
kafka 生产者实现类
 */

import java.util.Properties
import kafka.producer._

object KafkaProducer {

    private val sendTopic = "yangdecheng"
    private val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", "61.147.114.81:9092,61.147.114.82:9092,61.147.114.84:9092,61.147.114.80:9092,61.147.114.85:9092")
    props.put("request.required.acks","1")
//  props.put("replica.fetch.max.bytes", (1024 * 1024 * 20).toString)
//  props.put("message.max.bytes", (1024 * 1024 * 20).toString)

    private val producer = new Producer[String, String](new ProducerConfig(props))

    private  def kafkaMessage(message: String): KeyedMessage[String, String] = {
        new KeyedMessage(sendTopic, null, message)
    }

    def send(message: String): Unit = {
        try {
            producer.send(kafkaMessage(message))
        } catch {
            case e: Exception => println(e)
        }
    }
}