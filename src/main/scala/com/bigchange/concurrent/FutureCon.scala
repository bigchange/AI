package com.bigchange.concurrent

import java.util.concurrent.Callable

import scala.concurrent.ExecutionContext.Implicits.global
import akka.dispatch.Futures
import com.bigchange.thread.ThreadPool
import org.junit.Test

/**
  * Created by C.J.YOU on 2017/1/19.
  */
object FutureCon {

  var number = 0

  @Test
  val isEven: PartialFunction[String, String] = {
    case x if x.toInt % 2 == 0 => x + " is even"
  }

  def main(args: Array[String]) {

    for(i <- 0 to 10) {

      // 这里实现真正的并发
      val futrue = Futures.future(new Callable[String](){
        override def call(): String = {
          number += 1
          println(number)
          return number.toString
        }
      }, ThreadPool.executionContext)

      futrue.onSuccess[String](isEven)

    }


  }

}
