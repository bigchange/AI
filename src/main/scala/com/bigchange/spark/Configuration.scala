package com.bigchange.spark

/**
  * Created by C.J.YOU on 2017/1/18.
  */
class Configuration {

  // accept type
  type T = (String, String)

  // streaming show completed batch size
  val retainBatches = ("spark.streaming.ui.retainedBatches", "1000")

  val serializer = ("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val kryoSerializerBuffer = ("spark.kryoserializer.buffer.max", "2000")



}
