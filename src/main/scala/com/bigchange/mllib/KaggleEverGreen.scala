package com.bigchange.mllib

import org.apache.spark.mllib.classification.{NaiveBayes, SVMWithSGD, LogisticRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by CHAOJIANG on 2016/5/5 0005.
  * 网页中推荐页面是短暂还是长久的分类问题
  * 数据集：开始四列:URL、页面ID、原始文本内容和分配给页面的类别、接下来22列包含各种各样的数值或者属性类别、最后一列为目标值，-1 为长久，0为短暂
  */
object KaggleEverGreen {
  val sc = new SparkContext(new SparkConf().setAppName("Classification").setMaster("local"))
  def main(args: Array[String]) {
    //检查数据
    val rawData = sc.textFile(args(0))
    val records = rawData.map(line =>line.split("\t"))
    // 数据清理，替换而外的（“）和数据集中一些（？）代替的缺失数据，缺失数据本例中用0替换
    val data = records.map{ r =>
      val trimmed = r.map(_.replace("\"",""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4,r.size -1).map(d => if(d == "?") 0.0 else d.toDouble)
      LabeledPoint(label,Vectors.dense(features)) // 封装目标变量和特征向量
     }

    data.cache()
    val numData = data.count()

    // 朴素贝叶斯要求特征非负
    val naivebayesData = records.map{ r =>
      val trimmed = r.map(_.replace("\"",""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4,r.size - 1).map(d => if(d == "?") 0.0 else d.toDouble).map(d => if(d < 0) 0.0 else d )
      LabeledPoint(label,Vectors.dense(features))
    }

    // train model: 其他参数为默认，svm和lr设置了迭代次数，dt设置最大树深度
    val numIterations = 10
    val maxTreeDepth = 5 // for decisionTree
    // LR
    val lrModel = LogisticRegressionWithSGD.train(data,numIterations)
    // SVM
    val svmModel = SVMWithSGD.train(data,numIterations)
    // naivebayes
    val naiveModel = NaiveBayes.train(naivebayesData)
    // DT
    val dtModel = DecisionTree.train(data,Algo.Classification,Entropy,maxTreeDepth)

    // use model : prediction 数据集

    // evaluation model : 正确率，错误率，准确率和召回率，ROC，F-Measure
    // DT的预测阈值是需要明确给出的
    val dtTotalCorrect = data.map{point =>
      val score = dtModel.predict(point.features)
      val predicted = if(score > 0.5) 1 else 0
      if(predicted == point.label) 1 else 0
    }.sum
    val dtAccuracy = dtTotalCorrect / numData
    println(s"dt accuracy：$dtAccuracy") // dt accuracy：0.6482758620689655

  }
}
