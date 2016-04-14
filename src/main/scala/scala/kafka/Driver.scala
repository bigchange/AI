package scala.kafka

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.regex.{Pattern, Matcher}
import java.util.{Arrays, ArrayList, Date}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.storage.StorageLevel

object Driver {

  val expireDay = "1"
  val table = "table 1"
  val table2="table 2"


  def send2Kafka(key:String, value: String): Unit = {
    KafkaProducer.send("send message")
  }
  def send2Kafka2(key:String, value: String): Unit = {
    KafkaProducer.send("send message")
  }



  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
  def getTime: String = {
    sdf.format(new Date())
  }



  //get time : 201504071722, 年月日时分秒
  def get_curr_time: String = {
    val format = new SimpleDateFormat ("yyyyMMddHHmm")
    format.format(new Date())
  }

  var sequence_number = -1
  var last_modified_time = System.currentTimeMillis
  var skey_prefix = get_curr_time

  //work function for extracting useful fields.

  def main(args: Array[String]): Unit = {


   // System.setProperty("spark.default.parallelism", "150")
    System.setProperty("spark.shuffle.consolidateFiles", "true")
    // System.setProperty("spark.serializer", "org.apache.spark.serializer.KeyoSerializer")
    System.setProperty("spark.speculation", "true")
    System.setProperty("spark.streaming.concurrentJobs", "10")
    System.setProperty("spark.yarn.max.executor.failures", "99")
    System.setProperty("spark.streaming.blockInterval", "100")
    // System.setProperty("spark.akka.frameSize", "30")

    //System.setProperty("spark.network.timeout", "1200")
    //System.setProperty("spark.task.cpus", "5")  spark.shuffle.memoryFraction
    //  System.setProperty("spark.storage.memoryFraction", "0.7")
    //------------------------------
    // System.setProperty("spark.akka.threads", "16")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.rpc.numRetries", "6")
    System.setProperty("spark.executor.heartbeatInterval", "20")
    //System.setProperty("spark.shuffle.memoryFraction", "0")
    // System.setProperty("spark.yarn.executor.memoryOverhead", "5120")
    //System.setProperty("spark.yarn.driver.memoryOverhead", "5120")
    System.setProperty("spark.task.maxFailures", "8")

    System.setProperty("spark.network.timeout", "1000")

    val ssc = new StreamingContext(new SparkConf(), Seconds(60))
    val topicpMap = KafkaConf.topics.split(",").map((_, KafkaConf.numThreads.toInt)).toMap

    // kafka 通过定义多个接受器实现数据的高效并发接收，但需要注意 每个接受器需要有一个c至少一个cpu核支持，所以资源充足的时候可以考虑使用
    val numStrems = 5

    val kafkaStreams=(1 to numStrems).map{ i => KafkaConf.createStream(ssc, KafkaConf.zkQuorum, "spark_group", topicpMap).map(_._2) }

//   val ts: String = getTime
//   val linesdata=ssc.union(kafkaStreams).saveAsTextFiles("hdfs://ns1/user/hadoop/kunyan/"+ts+"/")

      val linesdata=ssc.union(kafkaStreams)

    ssc.start()
    ssc.awaitTermination()

  }
}
