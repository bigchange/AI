package com.bigchange.streaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random

/**
  * Created by C.J.YOU on 2016/9/27.
  * 网络连接消息发送端
  */
object SocketProducer {

  def main(args: Array[String]) {

    val random = new Random()

    val maxEvents = 6

    val names = Seq("Miguel","Eric","James","June","Doung","Frank","Jerry","Mike","karrdy","cythia")

    val products = Seq("iPhone Cover" -> 9.99, "HeadPhones" -> 5.49, "Samsung Galaxy" -> 8.95, "iPod Cover" -> 7.49)

    def generateProductorEvents(n: Int) = {

      (1 to n).map { i =>
        val (product, price) = products(random.nextInt(products.size))
        val user = random.shuffle(names).head
        (user, product, price)
      }
    }

    val listener = new ServerSocket(9999)
    println("listener port:" + listener.getLocalPort)

    while(true) {
      val socket = listener.accept()
      new Thread() {
        override def run() = {
          println("get client from:" + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream, true)

          while (true) {
            Thread.sleep(1000)
            val num = random.nextInt(maxEvents)
            val productEvents = generateProductorEvents(num)
            productEvents.foreach { events =>
              out.write(events.productIterator.mkString(","))
              out.write("\n")
            }
            out.flush()
            println(s"created $num events")
          }
          socket.close()
        }

      }.start()
    }


  }


}
