package scala.util

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.config.FileConfig
import scala.log.SUELogger

/**
  * Created by C.J.YOU on 2016/4/5.
  */

object KafkaUtil {

  def flatMapFun(line: String): mutable.MutableList[String] = {
    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    val res = StringUtil.parseJsonObject(line)
    if(res.nonEmpty){
      lineList.+=(res)
    }
    lineList
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        """
          |Usage: LoadData <RedisConf> <brokers> <topics> <zkhosts>
          |<brokers> is a list of one or more Kafka brokers
          |<topics> is a list of one or more kafka topics to consume from
          |<zkhosts> is a list of zookeeper to consume from

        """.stripMargin)
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setAppName("Data_Analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.cleaner.ttl", "10000")
      .setMaster("local")

     // val sc = new SparkContext(sparkConf)
    // 外部参数参入
    val Array(brokers, topics, zkhosts) = args

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // 可以通过 StreamingContext 来得到 SparkContext
    val sc = ssc.sparkContext

    val text = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "TelecomTest","zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),topics = topics.split(",").toSet)
    SUELogger.warn("write data")
    val result = text.flatMap(x =>flatMapFun(x._2))

    /** write data to local file */
    try {
      result.foreachRDD(rdd => {

        rdd.map(x  => x).filter(_!= null).zipWithIndex().foreach(record => println( TimeUtil.getDay + "_ky_"+ record._2 +","+record._1))

         /*val resArray = rdd.collect()
         val dir = FileConfig.test_dir + "/" + TimeUtil.getDay
         val file = FileConfig.test_dir + "/" + TimeUtil.getDay +"/"+TimeUtil.getCurrentHour
         FileUtil.mkDir(dir)
         FileUtil.createFile(file)
         FileUtil.writeToFile(file,resArray)
         // HBaseUtil.saveData(resArray)*/
      })
    } catch {
      case e:Exception =>
        SUELogger.error("save data error!!!!!!!!!!!!!!!!!!!!")
        System.exit(-1)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
