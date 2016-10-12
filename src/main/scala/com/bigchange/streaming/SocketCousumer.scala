package com.bigchange.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by C.J.YOU on 2016/9/27.
  */
object SocketConsumer {

  def main(args: Array[String]) {

    val ssc = new StreamingContext(new SparkConf().setAppName("SCOKETCONSUMER").setMaster("local"), Seconds(10))

    // 对有状态的操作，需要设置一个检查点
    ssc.checkpoint("file:///E:/checkpoint/")

    val stream = ssc.socketTextStream("localhost", 9999)

    val events = stream.map { record =>
      val event = record.split(",")
      (event(0), event(1), event(2).toDouble)
    }
    events.print()

    val users = events.map { case(user, product, price) => (user,(product, price)) }

    val revenuePerUser = users.updateStateByKey(updateState)
    revenuePerUser.foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    ssc.awaitTermination()

  }
  // 状态更新
  def updateState(prices:Seq[(String, Double)], currentTotal:Option[(Int, Double)]) = {

    val currentRevenue = prices.map(_._2).sum
    val currentNumberPurchases = prices.size
    val state = currentTotal.getOrElse((0, 0.0))
    Some((currentNumberPurchases + state._1, currentRevenue + state._2))

  }


}
