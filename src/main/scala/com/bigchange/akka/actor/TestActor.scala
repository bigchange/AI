package com.bigchange.akka.actor

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging
import com.bigchange.akka.message.MapData

/**
  * Created by C.J.YOU on 2016/8/16.
  */
class TestActor extends Actor {

  val log = Logging(context.system, this)

 /* override def receive: Receive = {
    case mes: String => // 如果收到的是字符消息通知MapActor
      log.info("master got message:" + mes)

    case data: AggData => // 如果收到的是AggData消息，告诉aggregateActor 处理
      log.info(" send message to other actor " )
    case _ =>
      log.info("master unhandled message")
      unhandled(_)
  }*/

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    // 初始化Actor代码块
  }


  // props
  val props1 = Props()
  val props2 = Props[TestActor]
  val props3 = Props(new TestActor)
  val props6 = props1.withDispatcher("my-dispatcher")
  // create actor
  val system = ActorSystem("MySystem")
  val myActor = system.actorOf(Props[TestActor].withDispatcher("my-dispatcher"), name = "myactor2")

  //使用匿名类创建Actor,在从某个actor中派生新的actor来完成特定的子任务时，可能使用匿名类来包含将要执行的代码会更方便
  def receive = {
    case m: MapData ⇒
      context.actorOf(Props(new Actor {
        def receive = {
          case Some(msg) ⇒
            val replyMsg = doSomeDangerousWork(msg.toString)
            sender ! replyMsg
            context.stop(self)
        }

        def doSomeDangerousWork(msg: String): String = { "done" }

      })) forward m
  }



}
