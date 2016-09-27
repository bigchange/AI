package com.bigchange.streaming

import java.io.PrintWriter
import java.net.ServerSocket

import breeze.linalg.DenseVector

import scala.util.Random

/**
  * Created by C.J.YOU on 2016/9/27.
  * 随机线性回归数据的生成器
  */
object StreamingModelProducer {

  def main(args: Array[String]) {

    val maxEvent = 100
    val numFeatures = 100
    val random = new Random()
    // 生成服从正太分布的稠密向量函数
    def generateRandomArray(n: Int) = Array.tabulate(n)(_ => random.nextGaussian())
    // 一个确定的随机模型权重向量
    val w = new DenseVector(generateRandomArray(numFeatures))
    val intercept = random.nextGaussian() * 10
    // 生成一些随机数据事件
    def generateNoisyData(n:Int) = {

      (1 to n).map { i  =>
        val x = new DenseVector(generateRandomArray(numFeatures)) // 随机特征向量
        val y = w.dot(x)
        val noisy = y + intercept // 目标值
        (noisy, x)
      }
    }

    // 创建网络生成器
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
            val num = random.nextInt(maxEvent)
            val productEvents = generateNoisyData(num)
            productEvents.foreach { case(y, x) =>
              out.write(y + "\t" + x.data.mkString(","))
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
