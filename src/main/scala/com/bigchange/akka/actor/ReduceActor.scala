package com.bigchange.akka.actor

import akka.actor.{ActorRef, UntypedActor}
import akka.event.Logging
import com.bigchange.akka.message.{MapData, ReduceData, Result, Word}

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/8/16.
  */
class ReduceActor(aggregateActor: ActorRef) extends  UntypedActor {

  // val aggregateActor = getContext().actorOf(Props(classOf[AggregateActor]), "aggregateActor")

  val log = Logging(context.system, this)

  @scala.throws[Throwable](classOf[Throwable])
  override def onReceive(message: Any): Unit = {

    message match  {
      case mes: String =>
        log.info("reduce got message:" + mes)
        aggregateActor.tell("reduce ok!",this.sender())

      case mapData: MapData =>
        aggregateActor ! reduceDataList(mapData.dataList)

      case message:Result =>
        println("ReduceActor:" + message.resultValue.toString())
        sender() ! message

      case _ =>
        log.info("map unhandled message")
        unhandled(message)
    }
  }


  // 统计key相同的个数
  def reduceDataList(dataList: List[Word]) :  ReduceData = {

    val hashMap = new mutable.HashMap[String, Int]()

    for(item <- dataList) {
      val total = hashMap.getOrElseUpdate(item.word,item.count)
      hashMap.update(item.word,total)
    }

    new ReduceData(hashMap)

  }

}
