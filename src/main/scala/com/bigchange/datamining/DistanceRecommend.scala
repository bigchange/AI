package com.bigchange.datamining

import breeze.numerics.{abs, pow, sqrt}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/9/8.
  * 一个推荐算法的核心部分(除了前期的数据清洗)
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

  // 标准化
  def normalization(arr:Array[Double]): Array[Double] = {

    val listBuffer = new ListBuffer[Double]
    // 获取中位数
    def getMedian(arr:Array[Double]): Double = {

      val sortd = arr.toList.sortWith(_ < _).toArray
      val len = arr.length
      if(len % 2 == 1) {
       sortd(len / 2)
      } else {
        (sortd(len / 2 - 1) + sortd(len / 2)) / 2
      }
    }
    // 计算ads
    def getAbsoluteStandardDeviation(array: Array[Double],median: Double): Double = {

      var sum = 0.0
      for (item <- array) {
        sum += abs(item - median)
      }
      sum / array.length * 1.0
    }

    val median = getMedian(arr)
    val ads = getAbsoluteStandardDeviation(arr, median)

    for (item <- arr) {
      listBuffer.+=((item - median) / ads)
    }
    listBuffer.toArray

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
            recommendRatingCompute.update(y.item, recommendRatingCompute(y.item) + y.value * x._2)
          }
        }
      })
    })

    recommendRatingCompute

  }

  // 修正的余弦相似度
  def fixedCosSimilarity(item1: String, item2: String, userList: Map[String,List[Rating]]): Double = {

    // 计算每个用户评分的均值
    val average = userList.map(x => (x._1, x._2.map(_.value).sum / (x._2.size * 1.0)))
    // println("average:" + average)
    val userRatingItem = userList.map(x => (x._1, x._2.map(x => (x.item, x.value)).toMap)).toList
    // 计算
    var upNum = 0.0
    var downX = 0.0
    var downY = 0.0
    userRatingItem.foreach(x =>
      if(x._2.contains(item1) && x._2.contains(item2)) {
        upNum +=((x._2(item1) - average(x._1)) * (x._2(item2) - average(x._1)))
        downX += pow(x._2(item1) - average(x._1), 2.0)
        downY += pow(x._2(item2) - average(x._1), 2.0)
      }
    )

    val res = upNum / (sqrt(downX) * sqrt(downY) * 1.0)

    res

  }

  // 预测用户user对物品item1的评分
  def predictRating(user: String, item1: String, userList: Map[String,List[Rating]]): Double = {

    // 所有的item
    val allItem = userList.flatMap(x => x._2.map(_.item)).toList
    //  计算item 与 item 物品相似度矩阵
    val itemCosSimMap = new mutable.HashMap[(String, String),Double]()
    allItem.foreach(x => allItem.foreach(y => if(x != y) itemCosSimMap.+=((x, y) -> fixedCosSimilarity(x, y, userList))))
    // 修正用户评分到（-1 ~ 1）,目前评分系统是5分制
    val max = 5
    val min = 1
    val userStandRating = userList(user).map(x => (x.item, (2 * (x.value - min) - (max - min)) / (max - min) * 1.0))
    // 计算
    var up = 0.0
    var down = 0.0
    val cal = userStandRating.foreach(x =>
      if(x._1 != item1) {
        up += x._2 * itemCosSimMap((x._1, item1))
        down += abs(itemCosSimMap((x._1, item1)))
      }
    )
    // println("up:down:" + up + "," + down  )
    val res = up / down
    println("预测评分:" + res)
    // 转换到5星评价体系
    (res + 1) * (max - min) / 2.0 + min

  }

  // slope One 算法: 关注有些差异值是不需要重新计算历史数据集，只需记录之前差值和记录同时评价过这对物品的用户数就可以了[逻辑暂时没有实现]
  // 第一步item计算差值
  def computeDeviations(item1: String, item2: String, userList: Map[String,List[Rating]]): Double = {

    // 评分转为Map
    val userRatingItem = userList.map(x => (x._1, x._2.map(x => (x.item, x.value)).toMap)).toList
    //  计算item 与 item 差值
    val itemDeviation = new mutable.HashMap[(String, String),Double]()
    val userCount = new mutable.HashMap[(String, String),Int]()

    userRatingItem.foreach { x =>

      if(x._2.contains(item1) && x._2.contains(item2)) {

        if(itemDeviation.contains((item1,item2))) {
          itemDeviation.update((item1,item2), x._2(item1) - x._2(item2) + itemDeviation.get((item1,item2)).get)
        } else {
          itemDeviation.+=(((item1,item2),x._2(item1) - x._2(item2)))
        }

        if(userCount.contains((item1,item2))){
          userCount.update((item1,item2),1 + userCount.get((item1,item2)).get)
        } else {
          userCount.+=(((item1,item2),1))
        }
      }
    }

    if(itemDeviation.contains((item1, item2)) && userCount.contains((item1, item2))) {
      itemDeviation((item1, item2)) / userCount((item1, item2))
    } else 0.0


  }
  // 加权的Slope One算法：推荐逻辑的实现
  def slopeOneRecommendations(user: String, item1: String, userList: Map[String,List[Rating]]): Double = {
    // 所有的item
    val allItem = userList.flatMap(x => x._2.map(_.item)).toList
    // 计算x,y 共同评分的用户
    val userRatingItem = userList.map(x => (x._2.map(x => x.item), x._1)).toList
    val userCount = new mutable.HashMap[(String, String),Int]()
    allItem.foreach(x =>
      allItem.foreach(y =>
        userRatingItem.foreach(i =>{
          if(i._1.contains(x) && i._1.contains(y)) {
            if(userCount.contains((x,y))) {
              userCount.update((x,y),1 + userCount.get((x,y)).get)
            } else {
              userCount.+=(((x,y),1))
            }
          }
        })
      ))

    //  计算item 与 item 差值矩阵
    val itemDeviMap = new mutable.HashMap[(String, String),Double]()
    allItem.foreach(x =>
      allItem.foreach(y =>
       if(x != y) {
         itemDeviMap.+=((x, y) -> computeDeviations(x, y, userList))
       }
      ))

    println("itemDeviMap: " + itemDeviMap)

    // 推荐
    var up = 0.0
    var down = 0.0
    userList(user).foreach { x =>
      val item = x.item
      val value = x.value
      if(item != item1) {
        if(itemDeviMap.contains((item1, item)) && userCount.contains((item1, item))){
          up += ((itemDeviMap((item1,item)) + value) * userCount((item1,item)))
          down += userCount((item1,item))
        }
      }
    }

    up / down

  }

}
