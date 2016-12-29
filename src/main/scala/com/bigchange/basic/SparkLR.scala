/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.bigchange.basic

import java.util.Random

import scala.math.exp
import breeze.linalg.{DenseVector, Vector}
import org.apache.spark._

/**
 * Logistic regression based classification.
 * Usage: SparkLR [slices]
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
 * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
 */
object SparkLR {

  val N = 10000  // Number of data points
  val D = 10   // Numer of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  // 定义操作的对象，如同：LabelPoint 的形式方便模型的处理
  case class DataPoint(x: Vector[Double], y: Double)

  // read data from specific format
  // data format：　label [ Dimension.... ]
  def parsePoint(line: String): DataPoint = {
    val tok = new java.util.StringTokenizer(line, " ")
    val y = tok.nextToken.toDouble
    val x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    DataPoint(new DenseVector(x), y)
  }

  def generateData: Array[DataPoint] = {
    //  生成 单个 dataPoint
    def generatePoint(i: Int): DataPoint = {
      val y = if (i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D){ rand.nextGaussian + y * R}
      DataPoint(x, y)
    }
    // 多个dataPoint
    Array.tabulate(N)(generatePoint)
  }

  def showWarning() {

    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
        |org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
        |for more conventional use.
      """.stripMargin(':'))
  }

  def main(args: Array[String]) {

    showWarning()

    val sparkConf = new SparkConf().setAppName("SparkLR").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val numSlices = if (args.length > 0) args(0).toInt else 2
    // random data
    // val points = sc.parallelize(generateData, numSlices).cache()

    // 指定data training
    val lines = sc.textFile(if(args.length > 1) args(1) else "")
    val points = lines.map(parsePoint).cache()

    // Initialize w to a random value
    var w = DenseVector.fill(D){ 2 * rand.nextDouble - 1}
    println("Initial w: " + w)
    // Initial w: DenseVector(-0.8066603352924779, -0.5488747509304204, -0.7351625370864459, 0.8228539509375878, -0.6662446067860872, -0.33245457898921527, 0.9664202269036932, -0.20407887461434115, 0.4120993933386614, -0.8125908063470539)


    for (i <- 1 to ITERATIONS) {

      println("On iteration " + i)
      val gradient = points.map { p =>
        p.x * (1 / (1 + exp( -p.y * w.dot( p.x ) )) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    // Final w: DenseVector(5816.075967498865, 5222.008066011391, 5754.751978607454, 3853.1772062206846, 5593.565827145932, 5282.387874201054, 3662.9216051953435, 4890.78210340607, 4223.371512250292, 5767.368579668863)
    sc.stop()
  }
}
// scalastyle:on println
