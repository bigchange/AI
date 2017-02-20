package com.bigchange.akka.actor

import java.util.StringTokenizer

import akka.actor.{ActorRef, UntypedActor}
import akka.event.Logging
import com.bigchange.akka.message.{MapData, Result, Word}

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/16.
  */
class MapActor(reduceActor: ActorRef)  extends  UntypedActor {

  // val reduceActorInMap = getContext().actorOf(Props(new ReduceActor(reduceActor)),"reduceActor") // akka://TestAkkaSystem/user/MasterActor/mapActor/reduceActor]

  val log = Logging(context.system, this)

  @scala.throws[Throwable](classOf[Throwable])
  override def onReceive(message: Any): Unit = {

    message match {
      case data: String => // 告诉 reduce 我map好了，map好的消息可用对应message实体进行封装
        // getSender().tell("reply to master !!",this.sender()) // 造成消息一直循环发送。需要手动处理结束
        log.info("map got message:" + data)
        reduceActor.tell("map ok!",this.sender())
        reduceActor ! evaluateExpression(data)

      case message:Result =>
        sender() !  message

      case _ =>
        println("map unhandled message")
        unhandled(message)
    }

  }


  // 切分消息进行初始化次数
  def evaluateExpression(line: String) : MapData = {

    val listBuffer = new ListBuffer[Word]
    val stringTokenizer = new StringTokenizer(line)
    val defaultCount = 1

    while(stringTokenizer.hasMoreTokens) {

      val tokenizer = stringTokenizer.nextToken().toLowerCase
      listBuffer.+=(new Word(tokenizer,defaultCount))

    }

    new MapData(listBuffer.toList)

  }
}
