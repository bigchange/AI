package com.bigchange.mllib

import breeze.linalg._
import breeze.numerics.pow
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by C.J.YOU on 2016/9/19.
  */
object CKMeans {

  // 初始化： 获取电影数据和题材的映射关系数据集： <（电影ID， （标题，题材））>  -> titlesAndGenres:RDD[(id,(title,genres))]
  val titlesAndGenres: RDD[(Int,(String,ArrayBuffer[String]))] = null // { 处理： 暂时为null }

  // RDD[(Int, Array[Double])] 使用推荐系统中得出用户和物品这两个因子向量可转化成聚类模型训练的输入
  def kMeansModel(productFeatures:RDD[(Int, Array[Double])], userFeatures:RDD[(Int, Array[Double])]): Unit = {
    // 提取相关的因素并转换
    val movieFactors = productFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }

    val movieVectors = movieFactors.map(_._2)

    val userFactors = userFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }

    val userVectors = userFactors.map(_._2)
    // 归一化: 观察特征向量的分布，进一步确定是否需要对训练数据进行归一化处理，RowMatrix
    val movieMatrix = new RowMatrix(movieVectors)
    val userMatrix = new RowMatrix(userVectors)

    val movieMatrixSummary = movieMatrix.computeColumnSummaryStatistics()
    val userMatrixSummary = userMatrix.computeColumnSummaryStatistics()
    // 可以对特征进行：均值，偏差, normL1, normL2，max，min，count等一系列处理
    val (moviveMeans, userMeans) = (movieMatrixSummary.mean, userMatrixSummary.mean)
    val (movieVariance, userVariance) = (movieMatrixSummary.variance, userMatrixSummary.variance)

    // k-means 训练
    val numClusters = 5
    val numIterator = 10
    val numRuns = 3

    val movieClusterModel = KMeans.train(movieVectors, numClusters, numIterator, numRuns)

    val userClusterModel = KMeans.train(userVectors, numClusters, numIterator, numRuns)

    // 聚类模型进行预测
    val movie1 = movieClusterModel.predict(movieVectors.first)
    println("kmeans predict : " + movie1)

    // 解释类别预测结果： k-均值，最小化目标函数是样本到类中心的欧拉距离之和
    // 距离之和
    def computeDistance(v1: Vector[Double], v2: Vector[Double]) = sum(pow(v1 - v2, 2))

    val titlesWithFactor = titlesAndGenres.join(movieFactors)
    val movieAssigned = titlesWithFactor.map { case (id, ((title, genres), vector)) =>

        val pred = movieClusterModel.predict(vector)
        val clusterCentre = movieClusterModel.clusterCenters(pred)
        val dist = computeDistance(Vectors.dense(clusterCentre.toArray).asInstanceOf[Vector[Double]], Vectors.dense(vector.toArray).asInstanceOf[Vector[Double]])

        (id, title, genres.mkString(" "), pred, dist)

    }

    // 得到每个类簇对应的集合key为类中心点
    val  clusterAssignments = movieAssigned.groupBy { case (id, title, genres, cluster, dist) => cluster }.collectAsMap()

    // 得到每个类簇中距离最近的电影
    clusterAssignments.toSeq.sortBy(_._1).foreach { x =>
      println("cluster:" + x._1)
      val m = x._2.toSeq.sortBy(_._5)
      println(m.take(5).map { case (_, title, genres, _, dis) => (title, genres, dis)}.mkString("\n"))
      println("=======\n")
    }

    // 用户特征向量：计算每个离中心用户近的，根据他们的打分或者其他可用的元数据，发现这些用户的共同之处

  }

}
