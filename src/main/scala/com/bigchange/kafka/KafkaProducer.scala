package com.bigchange.kafka

/*
kafka 生产者实现类
 */

import java.util.Properties
import kafka.producer._

object KafkaProducer {

    val sendTopic = "user top"
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", "10.5.30.5:9092,10.5.30.6:9092,10.5.30.7:9092,10.5.30.8:9092,10.5.30.9:9092,10.5.30.10:9092,10.5.30.11:2181,10.5.30.12:9092,10.5.30.13:9092,10.5.30.14:9092,10.5.30.15:9092")
    props.put("request.required.acks","1")
//  props.put("replica.fetch.max.bytes", (1024 * 1024 * 20).toString)
//  props.put("message.max.bytes", (1024 * 1024 * 20).toString)

    val producer = new Producer[String, String](new ProducerConfig(props))

    def kafkaMessage(message: String): KeyedMessage[String, String] = {
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