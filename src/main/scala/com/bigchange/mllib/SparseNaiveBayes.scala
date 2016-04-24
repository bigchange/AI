package com.bigchange.mllib

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by C.J.YOU on 2016/3/24.
  */
object SparseNaiveBayes {

  case class Params(input: String = null,
                     minPartitions: Int = 0,
                     numFeatures: Int = -1,
                     lambda: Double = 1.0)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SNB").setMaster("local")
    val sc = new SparkContext(conf)
    val params = Params()
    val minPartitions =
      if (params.minPartitions > 0) params.minPartitions else sc.defaultMinPartitions
    val examples =
      MLUtils.loadLibSVMFile(sc,args(0), params.numFeatures, minPartitions)
    // Cache examples because it will be used in both training and evaluation.
    examples.cache()

    // 保留交叉验证
    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0)
    val test = splits(1)

    val numTraining = training.count()
    val numTest = test.count()

    println(s"numTraining = $numTraining, numTest = $numTest.")

    val model = new NaiveBayes().setLambda(params.lambda).run(training)

    /*val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))
    val accuracy = predictionAndLabel.filter(x => x._1 == x._2).count().toDouble / numTest
    println(s"Test accuracy = $accuracy.") */

    /*test.map(x =>{
      val label = x.label
      val feature = x.features
      label + "\t" + model.predict(feature)
    }).foreach(println)*/
    sc.stop()
  }

}
