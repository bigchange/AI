package com.bigchange.datamining

import breeze.numerics.pow
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

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
    // ??
    0.0
  }

  // 余弦相似度的范围从1到-1，1表示完全匹配，-1表示完全相悖
  def cosSim(vector1: DoubleMatrix, vector2: DoubleMatrix): Double = {

    vector1.dot(vector2) / (vector1.norm2 * vector2.norm2)

  }

  // 找出距离最近的用户（其实该函数会返回一个用户列表，按距离排序）
  def computeNearestNeighbor(user:String, userList: Map[String,List[Rating]]): List[(String,Double)] = {

    var list = new ListBuffer[(String,Double)]
    userList.foreach(x => if(x._1 != user) list.+=((x._1, manhattan(userList(user),userList(x._1)))))
    list.toList.sortBy(_._2)

  }

  // recommend : 数据量大的话使用RDD
  def recommend(user:String, userList: Map[String,List[Rating]]): List[(String,Double)] = {

    var recommendList = new ListBuffer[(String,Double)]
    // 找到距离最近的用户
    val nearest = computeNearestNeighbor(user, userList).head
    // 自己评价的乐队
    val userItemList = userList(user).map(_.item).iterator.toList
    // 找出这位用户评价过
    val recommendRating = userList(nearest._1)
    // 找出这位用户评价过、但自己未曾评价的乐队
    recommendRating.foreach(x => if(! userItemList.contains(x.item)) { recommendList.+=((x.item,x.value)) })

    recommendList.sortBy(_._2).toList.reverse

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
