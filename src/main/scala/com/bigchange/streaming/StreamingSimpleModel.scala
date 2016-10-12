package com.bigchange.streaming

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by C.J.YOU on 2016/9/27.
  * 流回归模型
  */
object StreamingSimpleModel {

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local","test",Seconds(10))
    val stream = ssc.socketTextStream("localhost",9999)
    val numberFeatures = 100
    val zeroVector = DenseVector.zeros[Double](numberFeatures)
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1)
      .setStepSize(0.01)


    val labeledStream = stream.map { event =>
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y, features = Vectors.dense(features))
    }

    model.trainOn(labeledStream)
    // 使用DStream的转换算子
    val predictAndTrue = labeledStream.transform { rdd =>
     val latestModel = model.latestModel()
      rdd.map { point =>
        val predict = latestModel.predict(point.features)
        predict - point.label
      }
    }
    // 计算MSE
    predictAndTrue.foreachRDD { rdd =>
      val  mse = rdd.map(x => x * x).mean()
      val rmse = math.sqrt(mse)
      println(s"current batch, MSE: $mse, RMSE:$rmse")

    }
    ssc.start()
    ssc.awaitTermination()

  }
}
