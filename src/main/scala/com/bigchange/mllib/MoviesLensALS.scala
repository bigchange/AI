package com.bigchange.mllib

import breeze.linalg.SparseVector
import org.apache.log4j.{Logger, Level}
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import breeze.linalg._
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.jblas.DoubleMatrix

/**
  * Created by C.J.YOU on 2016/3/21.
  */
object MoviesLensALS {

  case class Params(
                     ratingsData: String = "J:\\github\\dataSet\\ml-1m\\ml-1m\\ratings.dat",
                     moviesData:String = "J:\\github\\dataSet\\ml-1m\\ml-1m\\movies.dat",
                     kryo: Boolean = false,
                     numIterations: Int = 10,
                     var lambda: Double = 0.1,
                     rank: Int = 20,
                     numUserBlocks: Int = -1,
                     numProductBlocks: Int = -1,
                     implicitPrefs: Boolean = false) extends AbstractParams[Params]

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean): Double = {

    def mapPredictedRating(r: Double): Double = {
      if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
    }
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }


  def main(args: Array[String]) {

    val conf = new SparkConf ()
      .setAppName (s"MovieLensALS with")
    .setMaster("local")
    val params = Params()

    if (params.kryo) {
      conf.registerKryoClasses (Array (classOf [scala.collection.mutable.BitSet], classOf [Rating]))
        .set ("spark.kryoserializer.buffer", "8m")
    }
    val sc = new SparkContext (conf)

    Logger.getRootLogger.setLevel (Level.WARN)

    val implicitPrefs = params.implicitPrefs
    /*
         * MovieLens ratings are on a scale of 1-5:
         * 5: Must see
         * 4: Will enjoy
         * 3: It's okay
         * 2: Fairly bad
         * 1: Awful
         * So we should not recommend a movie if the predicted rating is less than 3.
         * To map ratings to confidence scores, we use
         * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
         * entries are generally between It's okay and Fairly bad.
         * The semantics of 0 in this expanded world of non-positive weights
         * are "the same as never having interacted at all".
         */
    val ratings = sc.textFile (params.ratingsData).map { line =>
      val fields = line.split ("::")
      if (implicitPrefs) {
        Rating (fields (0).toInt, fields (1).toInt, fields (2).toDouble - 2.5)
      } else {
        Rating (fields (0).toInt, fields (1).toInt, fields (2).toDouble)
      }
    }.cache ()

    val moviesMap  = sc.textFile(params.moviesData).map{ line =>
      val fields = line.split("::")
      (fields(0).toInt,fields(1))
    }.collectAsMap()

    val numRatings = ratings.count ()
    val numUsers = ratings.map (_.user).distinct ().count ()
    val numMovies = ratings.map (_.product).distinct ().count ()

    println (s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    val splits = ratings.randomSplit (Array (0.8, 0.2))
    val training = splits (0).cache ()
    val test = if (params.implicitPrefs) {
      /*
       * 0 means "don't know" and positive values mean "confident that the prediction should be 1".
       * Negative values means "confident that the prediction should be 0".
       * We have in this case used some kind of weighted RMSE. The weight is the absolute value of
       * the confidence. The error is the difference between prediction and either 1 or 0,
       * depending on whether r is positive or negative.
       */
      splits (1).map (x => Rating (x.user, x.product, if (x.rating > 0) 1.0 else 0.0))
    } else {
      splits (1)
    }.cache ()

    val numTraining = training.count ()
    val numTest = test.count ()
    println (s"Training: $numTraining, test: $numTest.")

    ratings.unpersist (blocking = false)

    var minRmse = 100.0
    var bestLamda = 0.01
    // for(i <- 1 to 10){
      params.lambda = 0.1 // * i
      // ALS model
      val model = new ALS()
        .setRank (params.rank) // 因子的个数，低阶近似矩阵中隐含特征的个数。合理取值（10-200）
        .setIterations (params.numIterations) // 10次左右的迭代基本收敛
        .setLambda (params.lambda) // 正则化参数，控制模型出现过拟合，参数应该让通过非样本的测试数据进行交叉验证来调整
        .setImplicitPrefs (params.implicitPrefs)
        .setUserBlocks (params.numUserBlocks) //
        .setProductBlocks (params.numProductBlocks) //
        .run (training)
      //.save(sc,"F:\\datatest\\ai\\ALSModel")

      // recommendation item for user (allow recommend item for user and user for item )
      val userId = 22
      val topK = 10
      val topKItems = model.recommendProducts(userId,topK)
      val topProduct = model.recommendProductsForUsers(10)
      val topUser = model.recommendUsersForProducts(10)

      // 用户和物品的因子向量
      val userFeatures = model.userFeatures
      val productFeatures  = model.productFeatures

      // check recommend item for user
      val userRatingMoviesSize = ratings.keyBy(_.user).lookup(userId).size // userId 22 rated movies
      // list recommend movies for userId
      println(topK+" movies for user:"+userId)
      topKItems.map(ratings => (moviesMap(ratings.product),ratings.rating)).foreach(println)

      //  evaluation RMSE
      val rmse = computeRmse (model, test, params.implicitPrefs)
      // println (s"$i -> Test RMSE = $rmse.")
      if(rmse < minRmse) {
        minRmse = rmse
        // bestLamda = i
      }

    // }
    println(s"the best model of lamda:$bestLamda,RMSE:$minRmse")
    // 计算k值平均准确率
    // 物品因子矩阵： 广播出去用于后续计算
    val itemFactors = model.productFeatures.map{ case (id,factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    val imBroadcast = sc.broadcast(itemMatrix)
    // 计算每个用户的推荐
    val allRecs = model.userFeatures.map{ case(userID,array) =>
      val userVector = new DoubleMatrix(array)
      val scores = imBroadcast.value.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendedIds = sortedWithId.map(_._2 + 1).toSeq  // 物品id + 1 由于物品因子矩阵的编号为0开始
      (userID,recommendedIds)
    }
    val userMovies = ratings.map{ case Rating(user,product,rating) => (user,product)}.groupBy(_._1)
    val K = 10
    val MAPK = allRecs.join(userMovies).map{ case(userID,(predicted,actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual,predicted,K)
    }.reduce(_+_) / allRecs.count  // MAPK: 整个数据集上的平均准确率
    println(s"Mean Average Precision at K =" + MAPK)
    // MLlib 内置的评估函数 使用（RegressionMetrics,RankingMetrics）
    val predictionAndActual = training.map{ x =>
      val predicted = model.predict(x.user,x.product)
      (predicted,x.rating)
    }
    val regressionMetrics = new RegressionMetrics(predictionAndActual)
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = "+ regressionMetrics.rootMeanSquaredError)
    // MAP (Mean Average Precision ) equal  K 值比较大
    val predictionAndActualForRanking  = allRecs.join(userMovies).map{ case(userID,(predicted,actualWithIds)) =>
      val actual = actualWithIds.map(_._2)
      (predicted.toArray,actual.toArray)
    }
    val rankingMetrics = new RankingMetrics(predictionAndActualForRanking)
    println(s"Mean Average Precision =" + rankingMetrics.meanAveragePrecision) // almost same while K = 实际物品的总数

    // item to item
    // 创建向量对象 jblas.DoubleMatrix
    val itemId = 567
    val itemFactor  = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    cosineSimilarity(itemVector,itemVector) // 1.0 : 自己与自己的相似度为1.0 totally same
    // cal cosineSimilarity
    // using DoubleMatrix
    val sims = productFeatures.map{ case(id,factor) =>
       val factorVector = new DoubleMatrix(factor)
       val sim = cosineSimilarity(itemVector,factorVector)
      (id,sim)
    }
    // using SparseVector to cal cosineSimilarity
    val itemVector2 = Vector.apply(itemFactor)
    val sv1 = itemVector2.asInstanceOf[SV]
    val itemSparseVector = new SparseVector[Double](sv1.indices,sv1.values,sv1.size)

    // cosineSimilarity cal method
    val sims2 = productFeatures.map{ case (id,factor) =>
      val factorVector = Vector.apply(factor)
      val sv1 = factorVector.asInstanceOf[SV]
      val factorSparseVector = new SparseVector[Double](sv1.indices,sv1.values,sv1.size)
      val sim = itemSparseVector.dot(factorSparseVector) / (norm(itemSparseVector) * norm(factorSparseVector))
      (id,sim)
    }

    val sortedSims = sims.top(topK)(Ordering.by[(Int,Double),Double]{ case (id,similarity) => similarity })
    println(s"$itemId -> $topK simlarity movies:")
    sortedSims.take(topK).map{ case (id,similarity) =>(moviesMap(id),similarity) }
      .foreach(println)

    sc.stop ()
  }

  // 相似度的衡量方法：皮尔森相关系数，实数向量的余弦相似度，二元向量的杰卡德相似系数，这里介绍余弦相似度
  // 余弦相似度取值(-1 ~ 1)：向量的点积与向量范数 或长度（L2-范数）的乘积的商
  def cosineSimilarity(vector1:DoubleMatrix,vector2:DoubleMatrix):Double = {
      vector1.dot(vector2) / (vector1.norm2 * vector2.norm2)
  }

  // 计算K值平均准确率：衡量查询所返回的前k个文档的平均相关性,实际与预测的比较
  def avgPrecisionK(actual:Seq[Int],predicted:Seq[Int],k:Int):Double={
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for((p,i) <- predK.zipWithIndex){
      if(actual.contains(p)){
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if(actual.isEmpty){
      1.0
    }else {
      score / scala.math.min(actual.size,k).toDouble
    }
  }
}
