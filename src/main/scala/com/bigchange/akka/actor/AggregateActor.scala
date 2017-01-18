package com.bigchange.akka.actor

import akka.actor.{ActorRef, UntypedActor}
import akka.event.Logging
import com.bigchange.akka.message.{ReduceData, Result}

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/8/16.
  */
class AggregateActor(resultActor: ActorRef) extends  UntypedActor {

  val finalHashMap = new mutable.HashMap[String, Int]()

  val log = Logging(context.system, this)

  @scala.throws[Throwable](classOf[Throwable])
  override def onReceive(message: Any): Unit = {

    message match {
      case data: String =>
        log.info("Aggregate got message:" + data)
        log.info("Aggregate ok!")

      case reduceData:ReduceData =>
        aggregateInMemoryReduce(reduceData.reduceHashMap)
        println("path:" + sender().path)
        resultActor ! new Result(finalHashMap)  // 给ResultActor发送计算结果

      case message:Result =>
        println("AggregateActor:" + message.resultValue.toString())

      case _ =>
        log.info("map unhandled message")
        unhandled(message)
    }
  }

  // 聚合
  def aggregateInMemoryReduce(reduceMap: mutable.HashMap[String, Int]): Unit = {

    var count = 0
    reduceMap.foreach(x => {

      if(finalHashMap.contains(x._1)) {
        count = x._2
        count += finalHashMap.get(x._1).get
        finalHashMap.put(x._1,count)
      } else {
        finalHashMap.put(x._1,x._2)
      }

    })

  }
}
