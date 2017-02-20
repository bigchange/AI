package com.bigchange.akka

import akka.actor.{ActorSystem, Props}
import com.bigchange.akka.actor.MasterActor
import com.bigchange.akka.message.Respond

/**
  * Created by C.J.YOU on 2016/8/16.
  * 程序主入口
  */
object Scheduler {

  def main(args: Array[String]) {

    val driverPort = 7777
    val driverHost = "localhost"

    val actorName = "helloer"

    val system = ActorSystem("""sparkDriver""")
    val masterActor = system.actorOf(Props[MasterActor], name = "MasterActor")

    masterActor.tell("lets do action", masterActor)  // 实现了通过反馈消息：得到计算是否结束，并打印出结果

    Thread.sleep(3000)  // 等待计算结束通知

    println("main:"+ Respond.getInstance.getResult.resultValue.toString())

    // 获取反馈信息
    /*val timeOut = new Timeout(Duration.create(5,"seconds"))
    val future = Patterns.ask(masterActor, "lets do it again", timeOut)
    val result = Await.result(future,timeOut.duration)
    if(result.asInstanceOf[String].nonEmpty) {
      println("main:"+ Respond.getInstance.getResult.resultValue.toString())
    }*/

    // 消息转发
    // masterActor.forward("forward message!")

    // Thread.sleep(3000)  // 等待计算结束通知
    // 获取mapReduce后结果
    // masterActor ! new Result() // 告诉masterActor 我需要得到计算结果

    system.terminate() // 结束actor system

    /*[INFO] [08/18/2016 14:34:26.636] [TestAkkaSystem-akka.actor.default-dispatcher-5] [akka://TestAkkaSystem/user/MasterActor] master got message:lets do action
    [INFO] [08/18/2016 14:34:26.640] [TestAkkaSystem-akka.actor.default-dispatcher-6] [akka://TestAkkaSystem/user/MasterActor/reduceActor] reduce got message:map ok!
    [INFO] [08/18/2016 14:34:26.640] [TestAkkaSystem-akka.actor.default-dispatcher-6] [akka://TestAkkaSystem/user/MasterActor/aggregateActor] Aggregate got message:reduce ok!
    [INFO] [08/18/2016 14:34:26.640] [TestAkkaSystem-akka.actor.default-dispatcher-6] [akka://TestAkkaSystem/user/MasterActor/aggregateActor] Aggregate ok!
    注意: reduceActor,aggregateActor 都为 MasterActor监管，actor之间发送消息的实例需要和接受消息的实例需要一致，否则出现消息无法处理 */


  }

}
