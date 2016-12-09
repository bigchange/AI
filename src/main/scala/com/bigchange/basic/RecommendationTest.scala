package com.bigchange.basic

import breeze.numerics.{pow, sqrt}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
// import redis.RedisClient

import scala.language.postfixOps

/**
  * Created by C.J.YOU on 2016/4/22.
  */
object RecommendationTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: <files> <host> <port> <db>" + "\n" +
        "files - (string) user rating files, format is 'userID::movieID::rating::?'" + "\n" +
        "host - (string) redis host address" + "\n" +
        "port - (int) redis port address" + "\n" +
        "db - (int) redis database")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("ThirdPartyTest")
    val sc = new SparkContext(sparkConf)

    val files = args(0)
    val host = args(1)
    val port = args(2) toInt
    val db = args(3) toInt

    println(s"files: $files, host: $host, port: $port, db: $db")

    // 文件的格式是: userID, movieID, rating, ?, 我们不关注最后一个 column
    // 我们采用的是 Item-to-Item 算法，具体可以参考 paper http://www.cin.ufpe.br/~idal/rs/Amazon-Recommendations.pdf

    // first step: 得到 movie to user 的关系
    val movieToUserRating = sc.textFile(files).map(x => {
      val arr = x.split("::")
      (arr(1), (arr(0), arr(2) toDouble))
    }).persist()

    // second step: 针对每个 movie，对 user 的评分做归一化
    val movieToRatings = movieToUserRating.aggregateByKey(0.0)((sum, ur) => sum + pow(ur._2, 2.0), _ + _).
      map(x => (x._1, sqrt(x._2)))

    val userToMovieRatingReg = movieToUserRating.join(movieToRatings).map({
      case (movie, ((user, rating), sum)) => (movie, (user, rating / sum))
    }).map({
      case (movie, (user, rating)) => (user, (movie, rating))
    }).partitionBy(new HashPartitioner(10))

    // third step: 得到了每个 user 对 movie 的评价(归一化), 可以计算得到 movie 之间的关系了
    val movieToMovieScore = userToMovieRatingReg.join(userToMovieRatingReg).map({
      case (u1, ((m1, r1), (m2, r2))) => ((m1, m2), r1 * r2)
    }).reduceByKey(_ + _).map({
      case ((m1, m2), r) => (m1, (m2, r))
    }).groupByKey()

    movieToMovieScore.foreachPartition(records => {
      // implicit val formats = DefaultFormats
      // implicit val akkaSystem = akka.actor.ActorSystem()

      /*
      // 将结果存储到redis中
      val redis = RedisClient(host, port, None, Some(db))

      records.foreach(record => {
        val m1 = record._1
        val json = TopK[(String, Double), Double](50, record._2, _._2) toList
        val ser = compact(render(json))

        redis.set(m1, ser, Some(3600))
      })

      redis.stop()*/
    })

    sc.stop()
  }

}
