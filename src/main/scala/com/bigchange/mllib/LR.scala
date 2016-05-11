package com.bigchange.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{SVMWithSGD, LogisticRegressionWithSGD, LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.optimization.{L1Updater, SquaredL2Updater, SimpleUpdater}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by C.J.YOU on 2016/3/24.
  */
object LR {

  // define enumeration type for regParam
  object RegType extends Enumeration {
    type RegParamType = Value
    val NONE, L1, L2 = Value
  }

  import RegType._

  case class Params(
                     var input: String = null,
                     numIterations: Int = 100,
                     stepSize: Double = 1.0,
                     regType: RegParamType = L2,
                     regParam: Double = 0.01 ) extends AbstractParams[Params]

  def main(args: Array[String]) {

    val params = Params()
    val conf = new SparkConf().setAppName(s"LinearRegression with $params").setMaster("local")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    params.input = args(0)

    // load training data set
    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()
    //  split data into training (80%) and test(20%)
    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    examples.unpersist(blocking = false)

    // weights updater
    val updater = params.regType match {
      case NONE => new SimpleUpdater()
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }
    // linearRegression training algorithm using SGD,build model
    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(params.numIterations)
      .setStepSize(params.stepSize)
      .setUpdater(updater)
      .setRegParam(params.regParam)
    val model = algorithm.run(training)

    // logisticRegression using LBFGS
    val logisticLBFGS =  new LogisticRegressionWithLBFGS().setNumClasses(10)
      logisticLBFGS.optimizer
        .setNumIterations(params.numIterations)
    val logisticLBFGSModel = logisticLBFGS.run(training)

    // logisticRegression using SGD
    val logisticSGD = new LogisticRegressionWithSGD()
    logisticSGD.optimizer
      .setNumIterations(params.numIterations)
      .setStepSize(params.stepSize)
      .setUpdater(updater)
      .setRegParam(params.regParam)
    val logisticSGDModel = logisticSGD.run(training)
    val logisticSGDModel2 = LogisticRegressionWithSGD.train(training,params.numIterations,params.stepSize)

    // using SVMWithSGD
    val SVMWithSGDObject = new SVMWithSGD()
    SVMWithSGDObject.optimizer
      .setNumIterations(params.numIterations)
      .setStepSize(params.stepSize)
      .setUpdater(updater)
      .setRegParam(params.regParam)
    val SVMWithSGDModel = SVMWithSGDObject.run(training)
    SVMWithSGDModel.clearThreshold()
    val SVMWithSGDModel2 = SVMWithSGD.train(training,params.numIterations)
    // clean threshold
    SVMWithSGDModel2.clearThreshold()
    // test model on test data set return (prediction ,label)
    val predictionAndLabel = test.map { case LabeledPoint(label,features) =>
        val prediction = model.predict(features)
      (prediction,label)
    }
    // another training test data method
    /*val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))*/

    // get metrics evaluation
    val metrics = new MulticlassMetrics(predictionAndLabel)
    // evaluation params
    val precision = metrics.precision
    val recall = metrics.recall
    val fpr = metrics.falsePositiveRate(1.0)
    val tpr = metrics.truePositiveRate(1.0)
    val fmeasure = metrics.fMeasure(1.0)
    println(s"precision=$precision,racall = $recall,fpr = $fpr,tpr = $tpr,F-Measure = $fmeasure")

    // get evaluation with AUC
    val binaryMetrics = new BinaryClassificationMetrics(predictionAndLabel)
    val auRoc = binaryMetrics.areaUnderPR()
    println(s"Area Under ROC = $auRoc")

    // save model
    model.save(sc,"/model/LRSGD")
    // load model from saving path
    val sameModel = LogisticRegressionModel.load(sc,"/model/LRSGD")
    // label RMSE
    /*val loss = predictionAndLabel.map { case (p, l) =>
      val err = p - l
      err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numTest)

    println(s"Test RMSE = $rmse.")*/

    sc.stop()
  }

}
