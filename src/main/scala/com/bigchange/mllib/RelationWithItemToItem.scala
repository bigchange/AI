package com.bigchange.mllib

import breeze.numerics.{sqrt, pow}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by CHAOJIANG on 2016/4/24 0024.
  * data format: userId  movieId  rating ....
  */

object RelationWithItemToItem {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
      .setAppName("Item to Item")
      .setMaster("local"))
    // announce the top number of items to get
    val topK = 2

    val userItem = sc.textFile("/rating.dat")
      .map(_.split("\t")).map(x =>(x(0),x(1),x(2))).distinct().cache()
    // cal item -> (user,rating) and item -> sqrt(ratings)
    val itemUser = userItem.map(x => (x._2,(x._1,x._3.toDouble))).partitionBy(new HashPartitioner(20))
    // sqrt : 规整化 rating 的值
    val itemPowSqrt = userItem.map(x => (x._2,pow(x._3.toDouble,2.0))).reduceByKey(_+_).mapValues(x => sqrt(x))
    // cal item -> ((user,rating),sqrt(ratings)) => user -> (item,rating/sqrt(ratings))
    val userItemSqrt = itemUser.join(itemPowSqrt).map(x =>{
      val item = x._1
      val sqrtRatings = x._2._2
      val user = x._2._1._1
      val rating = x._2._1._2
      (user,(item,rating / sqrtRatings))
    })
    // cal the relation of item to item in user dimension => get the score of item to item which connection the relation of items
    val itemToItem = userItemSqrt.join(userItemSqrt).map(x =>{
      val item1 = x._2._1._1
      val rating1 = x._2._1._2
      val item2 = x._2._2._1
      val rating2 = x._2._2._2
      val score = rating1 * rating2
      if(item1 == item2){
        ((item1,item2),-1.0)
      }else{
        ((item1,item2),score)
      }
    })

    itemToItem.reduceByKey(_+_).map(x => (x._1._1,(x._1._2,x._2))).groupByKey().foreach(x => {
      val sourceItem = x._1
      val topItem = x._2.toList.filter(_._2 > 0).sortWith(_._2 > _._2).take(topK)
      println(s"item = $sourceItem,topK relative item list:$topItem")
    })
    sc.stop()
  }

}
