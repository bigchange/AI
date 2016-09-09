package com.bigchange.datamining

import breeze.numerics.pow
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/9/8.
  */
// item评分类
case class Rating(item: String, value: Double)
// 用户对item的评分
case class UserRating(userName: String, rating: Rating)

object DistanceRecommend {

  // 初始化数据集
  def initial(originData: RDD[String]): RDD[(String,Array[Rating])] = {

    originData.map(_.split(",")).map { x =>
      val userName = x(0)
      val ratings = x.slice(1,x.length - 1).filter(_ != "?").map(_.split(":")).map(x => new Rating(x(0),x(1).toDouble))
      (userName, ratings)
    }
  }

  // 计算曼哈顿距离
  def manhattan(rating1List:List[Rating], rating2List:List[Rating]) = {

    var distance = 0.0

    rating1List.foreach(x => rating2List.foreach(y => if(x.item == y.item) distance += math.abs(x.value - y.value)))

    distance

  }

  // 闵可夫斯基距离 : r = 1 ,曼哈顿距离，r = 2, 欧几里得距离
  def minkowski(rating1List:List[Rating], rating2List:List[Rating], r:Int ): Double = {

    var distance = 0.0

    rating1List.foreach(x => rating2List.foreach(y => if(x.item == y.item) distance += pow(math.abs(x.value - y.value),r)))

    pow(distance, 1.0 / r)

  }
  // 皮尔逊相关系数: 相关性,它的值在-1至1之间，1表示完全吻合，-1表示完全相悖
  def pearson(rating1List:List[Rating], rating2List:List[Rating]): Double = {

    val bothRatingList = new ListBuffer[(Double, Double)]
    // 先筛选出有共同评分的项
    rating1List.foreach(x => rating2List.foreach(y => if(x.item == y.item)  bothRatingList.+=((x.value, y.value))))

    println("bothRatingList:" + bothRatingList)

    val averageX = bothRatingList.map(_._1).sum / ( bothRatingList.size * 1.0 )
    val averageY = bothRatingList.map(_._2).sum / ( bothRatingList.size * 1.0)
    // 计算分子
    val up = bothRatingList.map(x => (x._1 - averageX) * (x._2 - averageY)).sum
    // 计算分母
    val tmp = bothRatingList.map(x => (pow(x._1 - averageX, 2),pow(x._2 - averageY, 2)))
    val x = pow(tmp.map(_._1).sum, 0.5)
    val y = pow(tmp.map(_._2).sum, 0.5)

    val down = x * y

    up / down * 1.0

  }

  // 余弦相似度的范围从1到-1，1表示完全匹配，-1表示完全相悖
  def cosSim(vector1: DoubleMatrix, vector2: DoubleMatrix): Double = {
    // 此处的用户评分向量应该是共有的部分
    vector1.dot(vector2) / (vector1.norm2 * vector2.norm2)

  }

  // 找出距离最近的用户（其实该函数会返回一个用户列表，按距离排序）
  def computeNearestNeighbor(user:String, userList: Map[String,List[Rating]], metric: (List[Rating] , List[Rating]) => Double): List[(String,Double)] = {

    var list = new ListBuffer[(String,Double)]
    userList.foreach(x => if(x._1 != user) list.+=((x._1, metric(userList(user),userList(x._1)))))
    list.toList.sortBy(_._2)

  }

  // 找出K最近邻用户，根据相似度的值给对应用户设定评分的决定权重，按照权重重新计算评分，找出最高评分。下面使用pearson系数确定K个邻近用户
  def computeKNearestNeighbor(user:String,
                              userList: Map[String,List[Rating]],
                              metric: (List[Rating] , List[Rating]) => Double,
                              k:Int) = computeNearestNeighbor(user, userList, metric).take(k)

  // recommend : 数据量大的话使用RDD
  def recommend(user:String, userList: Map[String,List[Rating]]): List[(String,Double)] = {

    var recommendList = new ListBuffer[(String,Double)]
    // 距离计算选择偏函数 1: 曼哈顿距离
    val distanceComp = minkowski(_:List[Rating], _:List[Rating], 1)
    // 找到距离最近的用户
    val nearest = computeNearestNeighbor(user, userList, distanceComp).head
    // 自己评价的乐队
    val userItemList = userList(user).map(_.item).iterator.toList
    // 找出这位用户评价过
    val recommendRating = userList(nearest._1)
    // 找出这位用户评价过、但自己未曾评价的乐队
    recommendRating.foreach(x => if(! userItemList.contains(x.item)) { recommendList.+=((x.item,x.value)) })

    recommendList.sortBy(_._2).toList.reverse

  }

  // k最邻近算法推荐
  def kRecommend(user:String, userList: Map[String,List[Rating]], k:Int): mutable.HashMap[String,Double] = {

    var userPearsonList = new ListBuffer[(String,Double)]
    // 计算任意用户之间的pearson
    userList.foreach(x => if(x._1 != user) userPearsonList.+=((x._1, pearson(x._2, userList(user)))))
    // 获取k个最相关的用户
    val kUsers = userPearsonList.sortBy(_._2).toList.reverse.take(k)
    println("Kuser: " + kUsers )
    val totalWeight = kUsers.map(_._2).sum
    val userWeight = kUsers.map(x => (x._1, x._2 / (1.0 * totalWeight)))
    println("userWeight:" + userWeight)
    // 根据权重计算评分: 先去除用户user已经评分的
    // user自己评价的乐队集合
    val userItemList = userList(user).map(_.item).iterator.toList
    // 计算 评分
    val recommendRatingCompute = new mutable.HashMap[String, Double]()

    userWeight.foreach( x => {
      userList(x._1).foreach( y => {
        // 自己没有评价过
        if( !userItemList.contains(y.item)) {
          
          if(!recommendRatingCompute.contains(y.item)) {
            recommendRatingCompute.+=((y.item, y.value * x._2))
          } else {
            recommendRatingCompute.update(y.item, recommendRatingCompute.get(y.item).get + y.value * x._2)
          }
        }
      })
    })

    recommendRatingCompute

  }

  // val sc = SparkContext.getOrCreate(new SparkConf().setAppName("Test").setMaster("local"))

  // main 函数
  def main(args: Array[String]) {

    val user1 = "Hailey,Broken Bells:4.0,Deadmau5:1.0,Norah Jones:4.0,The Strokes:4.0,Vampire Weekend:1.0"
    val user2 = "Veronica,Blues Traveler:3.0,Norah Jones:5.0,Phoenix:4.0,Slightly Stoopid:2.5,The Strokes:3.0"
    val userList = List(user1, user2)

    // val userData = initial(sc.parallelize(userList))
    // userList.foreach(println)

  }


}
