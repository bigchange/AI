package com.bigchange.akka.actor

import akka.actor.UntypedActor
import com.bigchange.akka.message.{Respond, Result}


/**
  * Created by C.J.YOU on 2016/8/31.
  * 用来获取结果的actor
  */
class ResultActor extends  UntypedActor {

  override def onReceive(message: Any): Unit = {

    message match {

      case message:String =>
        sender() ! "Got it Result"

      case message: Result =>
        Respond.getInstance.setResult(message)
        println("ResultActor:" + message.resultValue.toString())
    }
  }

}
