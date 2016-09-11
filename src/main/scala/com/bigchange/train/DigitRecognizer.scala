package com.bigchange.train

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by C.J.YOU on 2016/5/9.
  * 用于数值识别的贝叶斯算法
  */
object DigitRecognizer {

  // diff lambda function
  def trainNBWithParams(input:RDD[LabeledPoint],lambda:Double,modelType:String) = {
    val nbModel = new NaiveBayes()
      .setLambda(lambda)
      .setModelType(modelType)
      .run(input)
    nbModel
  }

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
      .setAppName("DigitRe")
      .setMaster("local")
      )

    // checek data
    val allData = sc.textFile(args(0)).map(_.split(","))
    val data = allData.map{ lines =>
      val label = lines(0).toDouble
      val features = lines.slice(1,lines.length).map(_.toDouble)
      LabeledPoint(label,Vectors.dense(features))
    }.randomSplit(Array(0.8,0.2))

    // 使用交叉验证法训练数据和检验数据
    val trainData = data(0).cache()
    val testData = data(1).cache()

    /*val lambda = 1.0
    val modelType = "multinomial"
    val nbModel = new NaiveBayes()
      .setLambda(1.0)
      .setModelType(modelType)
      .run(trainData)*/

    val predictResult = Seq(0.001,0.01,0.1,1.0,10.0).map { param =>
      val nbModel = trainNBWithParams(testData,param,"multinomial")
      val predictResult =  testData.map { labeledPoint =>
        val predicted = nbModel.predict(labeledPoint.features)
        if (predicted > 0.5) 1 else 0
      }.reduce(_ + _)
      val accuracy = predictResult / testData.count * 1.0
      println(s"nb model with lambda:$param,modelTpye:multinomial,Accuracy:$accuracy")
    }
  }
}
