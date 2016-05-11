package com.bigchange.ml

import java.io.File

import org.apache.log4j.{Logger, Level}
import org.apache.spark.mllib.recommendation.{Rating, ALS, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source
// 协同过滤（推荐系统）
// ALS 模型：交叉最小二乘法
object MovieLensALS {

  def main(args: Array[String]) {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length != 2) {
      println("Usage: /path/to/spark/bin/spark-submit --driver-memory 2g --class week6.MovieLensALS " +
        "target/scala-*/movielens-als-ssembly-*.jar movieLensHomeDir personalRatingsFile")
      sys.exit(1)
    }

    //设置运行环境
    val conf = new SparkConf().setAppName("MovieLensALS")
      .setMaster("local")
    val sc = new SparkContext(conf)

    //装载用户评分，该评分由评分器生成
    val myRatings = loadRatings(args(0)+"/personalratings.dat")
    val myRatingsRDD = sc.parallelize(myRatings, 1)


    //样本数据目录
    val movieLensHomeDir = args(0)

    //装载样本评分数据，其中最后一列Timestamp取除10的余数作为key，Rating为值,即(Int,Rating)
    val ratings = sc.textFile(args(0)+"/ratings.dat").map { line =>
      val fields = line.split("::")
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    //装载电影目录对照表（电影ID -> 电影标题）
    val movies = sc.textFile(args(0)+"/movies.dat").map { line =>
      val fields = line.split("::")
      (fields(0).toInt, fields(1))
    }.collect().toMap

    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numMovies = ratings.map(_._2.product).distinct().count()
    // Got 10000054 ratings from 69878 users on 10677 movies.
    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")

    //将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)（6:2:2）
    //该数据在计算过程中要多次应用到，所以cache到内存
    val numPartitions = 4
    // 训练 （60%）
    val training = ratings.filter(x => x._1 < 6)
      .values
      .union(myRatingsRDD) //注意ratings是(Int,Rating)，取value即可
      .repartition(numPartitions)
      .cache()
    // 校验 （20%）
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      .cache()
    // 测试 （20%）
    val test = ratings.filter(x => x._1 >= 8).values.cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()
    // Training: 16002527, validation: 1999675, test: 1997906
    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)


    //训练不同参数下的模型，并在校验集中验证，获取最佳参数下的模型
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")

      /* RMSE (validation) = 0.8020619758188713 for the model trained with rank = 8, lambda = 0.1, and numIter = 10.
       RMSE (validation) = 0.7974995222123513 for the model trained with rank = 8, lambda = 0.1, and numIter = 20.
       RMSE (validation) = 3.6696552508471094 for the model trained with rank = 8, lambda = 10.0, and numIter = 10.
       RMSE (validation) = 3.6696552508471094 for the model trained with rank = 8, lambda = 10.0, and numIter = 20.
       RMSE (validation) = 0.7981921184268205 for the model trained with rank = 12, lambda = 0.1, and numIter = 10.
       RMSE (validation) = 0.792021981230906 for the model trained with rank = 12, lambda = 0.1, and numIter = 20.
       RMSE (validation) = 3.6696552508471094 for the model trained with rank = 12, lambda = 10.0, and numIter = 10
       RMSE (validation) = 3.6696552508471094 for the model trained with rank = 12, lambda = 10.0, and numIter = 20.
       The best model was trained with rank = 12 and lambda = 0.1, and numIter = 20, and its RMSE on the test set is 0.7920916479200906.
       The best model improves the baseline by 25.26%.*/


      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    // 保存最佳训练的模型
    // bestModel.get.save(sc,args(0)+"/ALSModel")

    //用最佳模型预测测试集的评分，并计算和实际评分之间的均方根误差
    val testRmse = computeRmse(bestModel.get, test, numTest)

    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    // create a naive baseline and compare it with the best model
    val meanRating = training.union(validation).map(_.rating).mean
    val baselineRmse =
      math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    // 推荐前十部最感兴趣的电影，注意要剔除用户已经评分的电影
    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = bestModel.get
      .predict(candidates.map((0, _)))
      .collect()
      .sortBy(-_.rating)
      .take(10)

    var i = 1
    println("Movies recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + movies(r.product))
      i += 1
    }

    //结束
    sc.stop()
  }

  /** 校验 集预测数据和 实际数据之间的均方根误差 **/
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  /** 装载用户评分文件 **/
  def loadRatings(path: String): Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()
    val ratings = lines.map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if (ratings.isEmpty) {
      sys.error("No ratings provided.")
    } else {
      ratings.toSeq
    }
  }
}