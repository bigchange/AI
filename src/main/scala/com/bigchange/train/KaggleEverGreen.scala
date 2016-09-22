package com.bigchange.train

import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.rdd.RDD
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
      val label = trimmed(r.length - 1).toInt
      val features = trimmed.slice(4,r.length -1).map(d => if(d == "?") 0.0 else d.toDouble)
      LabeledPoint(label,Vectors.dense(features)) // 封装目标变量和特征向量
     }

    data.cache()
    val numData = data.count()

    // 朴素贝叶斯要求特征非负
    val naivebayesData = records.map{ r =>
      val trimmed = r.map(_.replace("\"",""))
      val label = trimmed(r.length - 1).toInt
      val features = trimmed.slice(4,r.length - 1).map(d => if(d == "?") 0.0 else d.toDouble).map(d => if(d < 0) 0.0 else d )
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

    /**
      * evaluation model : 正确率，错误率，准确率和召回率（PR），ROC曲线，F-Measure
      */
    // DT的预测阈值是需要明确给出的
    val dtTotalCorrect = data.map{point =>
      val score = dtModel.predict(point.features)
      val predicted = if(score > 0.5) 1 else 0
      if(predicted == point.label) 1 else 0
    }.sum
    val dtAccuracy = dtTotalCorrect / numData
    println(s"dt accuracy：$dtAccuracy") // dt accuracy：0.6482758620689655
    // PR 和 ROC曲线下的面积
    val metrics = Seq(lrModel,svmModel).map { model =>
        val scoreAndLabels= data.map { point =>
          (model.predict(point.features),point.label)
        }
       val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())
      }

    val nbMetrics = Seq(naiveModel).map { model =>
      val scoreAndLabels= naivebayesData.map { point =>
        val score = model.predict(point.features)
        (if( score > 0.5) 1.0 else 0.0 ,point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())
    }

    val dtMetrics = Seq(dtModel).map { model =>
      val scoreAndLabels= data.map { point =>
        val score = model.predict(point.features)
        (if( score > 0.5) 1.0 else 0.0 ,point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())
    }
    val allMetrics = metrics ++ nbMetrics ++ dtMetrics
    allMetrics.foreach { case (modelType,areaPR,areaROC) =>
      // println(x = f"$modelType,Area Under PR:${areaPR * 100.0}%2.4%%,Area Under ROC:${areaROC * 100.0}%2.4%%")
    }
    // 类似计算性能的类MulticlassMetrics

    /**
      * 模型改进性能和参数调优
      */
    // 特征标准化：假设特征满足正态分布形式（不满足的时候转换），将特征向量用行矩阵表示，每个向量为分布式矩阵的一行
    val vectors = data.map(lp => lp.features)
    val rowMatrix = new RowMatrix(vectors)
    val rowMatrixSummary = rowMatrix.computeColumnSummaryStatistics() // 计算每列的统计信息方法
    // 计算矩阵每列的均值，最小，最大，方差，非0项个数
    println(rowMatrixSummary.mean)
    // 标准化操作: (x - u) / sqrt(variance)
    val scaler = new StandardScaler(withMean = true,withStd = true)
      .fit(vectors)
    val scaledData = data.map { lp =>
      LabeledPoint(lp.label,scaler.transform(lp.features))
    }
    println("standard normalized finished")
    // 使用标准化后的特征向量重复训练模型，查看各种评估值的变化
    // ...... : 明显发现相应的评估指标有所提高

    // 其他特征：对模型的影响，比如，类别特征
    // 对类别特征做1-of-k编码

    val categories = records.map( r => r(3)).distinct.collect().zipWithIndex.toMap
    // 使用定义的特征映射函数完成
    val categories_map = getFeatureMapping(records, 3) // 将第三列的类别特征装换为编码形式

    val numCategories = categories.size
    println(s"添加的类别特征范围大小:"+ numCategories)
    val dataCategories = records.map{r =>
      val trimmed = r.map(_.replaceAll("\"",""))
      val label = trimmed(r.length - 1).toInt
      val categoryIdx  = categories(r(3)) // 原始数据集中第四列类别特征提取
      /** Creates array with given dimensions */
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx) = 1.0 // 对应特征的位置下标值为1.0，其余为0.0
      val otherFeatures = trimmed.slice(4,r.length - 1).map(d => if(d == "?") 0.0 else d.toDouble)
      val features = categoryFeatures.++(otherFeatures)
      LabeledPoint(label,Vectors.dense(features))
    }

    /** 同样需要经过:标准化转换，训练相应的模型，最后查看评估值 */
    // 模型参数的调优
    // welcome back on Web,7th September, today is a big day. i decide to back to AI

  }

  /*
    * 这里的数据都是归一化后的实数变量类型，当存在像本数据集中类别这种类型变量时，我们需要
    * 将每个特征表示成二维的形式，我们将特征值映射到二元向量中非零的位置，定义一个这样的映射函数完成该功能：
    */
  def getFeatureMapping(rdd: RDD[Array[String]], index: Int) = {
    rdd.map(x => x(index)).distinct().zipWithIndex.collectAsMap()
  }
  // 特征转换为编码后的向量
  def featureToVector(featureMap:Map[String,Int], feature: String) = {
    val idx  = featureMap(feature) // 原始数据集中类型特征提取
    /** Creates array with given dimensions */
    val featureArray = Array.ofDim[Double](featureMap.size)
    featureArray.update(idx,1.0)// 对应特征的位置下标值为1.0，其余为0.0
    featureArray
  }

}
