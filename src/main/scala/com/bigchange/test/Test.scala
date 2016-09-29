package com.bigchange.test

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by C.J.YOU on 2016/9/27.
  */
object Test {

  val  sc = SparkContext.getOrCreate(new SparkConf().setMaster("local").setAppName("Test"))

  def main(args: Array[String]) {

    // @Test1 index唯一性测试
    // val rdd2 = sc.makeRDD(Seq("A","B","R","D","F","G","H","I","J","K","L","M","N"),4)

    // rdd2.zipWithIndex().foreach(println)

    // rdd2.zipWithUniqueId().foreach(println)

    // @Test 中心化和零均值中心化
    val rdd = sc.makeRDD(Seq(Array(1.0,2.0,3.0),Array(4.0,5.0,6.0),Array(7.0,8.0,9.0)))

    val vectors = rdd.map(x => Vectors.dense(x))

    val rowMatrix = new RowMatrix(vectors)

    /**
      * [1.0,2.0,3.0]
      * [4.0,5.0,6.0]
      * [7.0,8.0,9.0]
      */
    rowMatrix.rows.foreach(println)

    val means = rowMatrix.computeColumnSummaryStatistics().mean

    println("means:"+ means)  // means:[4.0,5.0,6.0]



  }

}
