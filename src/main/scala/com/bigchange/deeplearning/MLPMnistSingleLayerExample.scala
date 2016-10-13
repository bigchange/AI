package com.bigchange.deeplearning

/**
  * Created by C.J.YOU on 2016/10/13.
  * MNIST (hand writing)
  */
object MLPMnistSingleLayerExample {

  val numRows = 28 // The number of rows of a matrix.
  val numColumns = 28 // The number of columns of a matrix.
  val outputNum = 10 // Number of possible outcomes (e.g. labels 0 through 9).
  val batchSize = 128 // How many examples to fetch with each step.
  val rngSeed = 123 // This random-number generator applies a seed to ensure that the same initial weights are used when training. We’ll explain why this matters later.
  val numEpochs = 15 // An epoch is a complete pass through a given dataset.

}
