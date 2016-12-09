package com.bigchange.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by C.J.YOU on 2016/3/16.
  */

object SimpleTextClassificationPipeline {

  case class LabeledDocument(id:Long,text:String,label:Double)

  case class Document(id:Long,text:String)

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("STCP")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // 准备训练文档，这些文档已被标记
    val trainnig  = sc.parallelize(Seq(
      LabeledDocument(0,"层 次",1.0),
      LabeledDocument(1," b d",0.0),
      LabeledDocument(2,"spark f g h",0.0),
      LabeledDocument(3,"这种 分类",1.0),
      LabeledDocument(4,"kun yan data",1.0)))

    // Prepare training documents from a list of (id, text, label) tuples.
    val docs = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0))).toDF("id", "text", "label")


    // 配置 ML 的流程，包括三个阶段：Tokenizer（将输入的字符转为小写，并按空格分隔开），hashingTF(用散列法映射到其长期频率)，LogisticRegression（逻辑回归，分类）
    val tokenizer  = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(10)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    // println(hashingTF.toString())

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)  // 设置正则化参数

    val pipeline  = new Pipeline()
      .setStages(Array(tokenizer,hashingTF,lr))

    // 让流程去训练文档
    val model = pipeline.fit(trainnig.toDF())

    // Prepare test documents, which are unlabeled.
    val test = sc.parallelize(Seq(
      // Document(4L, words.mkString(" ")),
      Document(5L, "这种分类 层次"),
      Document(6L, "spark hadoop spark"),
      Document(7L, "这种分类")))

    // 让训练的模型 去预测test文档
    model.transform(test.toDF())
      // .show(10)
      .select("id","text","probability","features","prediction")
      .collect()
      .foreach { case Row(id:Long,text:String,prob:Vector,features:Vector,prediction:Double) =>
        println(s"($id,$text) --> prob = $prob,features =$features \n, prediction=$prediction")
      }

    val modelDoc = pipeline.fit(docs)

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

    // One can also combine ParamMaps.
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    val model2 = lr.fit(docs, paramMapCombined)

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test2 = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents.
    model2.transform(test2)
      .select("id", "text", "myProbability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    // Now we can optionally save the fitted pipeline to disk
    model2.write.overwrite().save("/tmp/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    // And load it back in during production
    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)  // Use 3+ in practice

    // compare with TrainValidationSplit

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(docs)

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test3 = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    cvModel.transform(test3)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

    // 读入分词后的文本数据
    /*val  iks = new IKSegmentation(Source.fromFile(new File(args(0))).bufferedReader(),true)
    var lexeme = iks.next()
    val words  = new ListBuffer[String]
    while(lexeme != null){
      words.+=(lexeme.getLexemeText)
      lexeme = iks.next()
    }*/
    // println(words.mkString(" "))

    sc.stop()

  }
}
