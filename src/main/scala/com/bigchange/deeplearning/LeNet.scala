package com.bigchange.deeplearning

import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.inputs.InputType
import org.deeplearning4j.nn.conf.layers.{ConvolutionLayer, DenseLayer, OutputLayer, SubsamplingLayer}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.lossfunctions.LossFunctions

/**
  * Created by C.J.YOU on 2016/10/13.
  * MNIST (hand writing)
  */
object LeNet {

  def main(args: Array[String]) {

    val nChannels = 1 // Number of input channels
    val numRows = 28 // The number of rows of a matrix.
    val numColumns = 28 // The number of columns of a matrix.
    val outputNum = 10 // Number of possible outcomes (e.g. labels 0 through 9).
    val batchSize = 100 // How many examples to fetch with each step.
    val rngSeed = 123 // This random-number generator applies a seed to ensure that the same initial weights are used when training. We’ll explain why this matters later.
    val numEpochs = 15 // An epoch is a complete pass through a given dataset.

    // load data
    val mnistTrain = new MnistDataSetIterator(batchSize, true, 12345)
    val mnistTest = new MnistDataSetIterator(batchSize, false, 12345)

    // construct cnn
    val builder = new NeuralNetConfiguration.Builder()
      .seed(rngSeed)
      .iterations(100)
      .regularization(true).l2(5e-4) // lambda = 5e-4
      .learningRate(1e-2) // .biasLearningRate(0.02)
      .weightInit(WeightInit.XAVIER)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .updater(Updater.NESTEROVS).momentum(0.9)
      .list
      .layer(0, new ConvolutionLayer.Builder(5 , 5).nIn(nChannels).stride(1,1).nOut(20).activation("relu").build()) // 32 x 32 -> 28 x 28 x 20
      .layer(1, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX).kernelSize(2 ,2).stride(2, 2).build())  // 28 x28 -> 14 x 14 x 20
      .layer(2, new ConvolutionLayer.Builder(5 , 5).stride(1,1).nOut(50).activation("relu").build()) // 14 x 14 x 20 -> 10 x 10 x 50
      .layer(3, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX).kernelSize(2 ,2).stride(2, 2).build()) // 10 x 10 x 50 -> 5 x 5 x 50
      .layer(4, new DenseLayer.Builder().activation("relu").nOut(500).build())  // 5 x 5 x 50  -> 1 x 1 x 500
      .layer(5, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD).nOut(outputNum).activation("softmax").build()) // 1 x 1 x 500 -> 1 x 1 x 10
      .setInputType(InputType.convolutionalFlat(28,28,1))
      .backprop(true).pretrain(false)

    val conf = builder.build()
    val model = new MultiLayerNetwork(conf)
    model.init()

    // train model
    model.setListeners(new ScoreIterationListener(1))

    for(i <- 0 until numEpochs) {

      model.fit(mnistTrain)
      println("Completed epoch:"+ i)
      println("Evaluate model....")
      val eval = new Evaluation(outputNum)

      while(mnistTest.hasNext) {

        val ds = mnistTest.next()
        val output = model.output(ds.getFeatureMatrix, false)
        eval.eval(ds.getLabels, output)

      }
      println(eval.stats())
      mnistTest.reset()

    }


  }






}
