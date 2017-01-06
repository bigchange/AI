package com.bigchange.kafka

/*
kafka 生产者实现类
 */

import java.util.Properties

import com.bigchange.config.Parameter
import kafka.producer._

/**
  * kafka 生产者
  *
  * @param parameter 外部参数传入
  */
class KafkaProducer(parameter: Parameter) {

    private val sendTopic = parameter.getParameterByTagName("kafka.sendTopic")

    private val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", parameter.getParameterByTagName("kafka.brokerList"))
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

object KafkaProducer {

    private  var kp: KafkaProducer = null

    def apply(parameter: Parameter): KafkaProducer = {

        if (kp == null)
            kp = new KafkaProducer(parameter)
        kp
    }

    def getInstance = kp

}