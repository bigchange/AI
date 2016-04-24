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
package com.bigchange.mllib

import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

/**
 * An example k-means app. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DenseKMeans [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DenseKMeans {

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import InitializationMode._

  case class Params(
      var input: String = null,
      k: Int = 2,
      numIterations: Int = 10,
      initializationMode: InitializationMode = Parallel) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()
    defaultParams.input = args(0)
    run(defaultParams)

  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"DenseKMeans with $params").setMaster("local")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = sc.textFile(params.input).map { line =>
      Vectors.dense(line.split(' ').map(_.toDouble))
    }.cache()

    val numExamples = examples.count()

    println(s"numExamples = $numExamples.")

    val initMode = params.initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }

    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(params.k)
      .setMaxIterations(params.numIterations)
      .run(examples)

    //  Return the K-means cost (sum of squared distances of points to their nearest center) for this
    val cost = model.computeCost(examples)
    // 获取质点(k个)
    val centerPoint = model.clusterCenters
    val one = centerPoint(0)
    val two  =  centerPoint(1)
    println(s"centerPoint=$one,$two.")
    println(s"Total cost = $cost.")

    sc.stop()
  }
}
// scalastyle:on println
