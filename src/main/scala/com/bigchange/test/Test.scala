package com.bigchange.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by C.J.YOU on 2016/9/27.
  */
object Test {

  val  sc = SparkContext.getOrCreate(new SparkConf().setMaster("local").setAppName("Test"))

  def main(args: Array[String]) {

    val rdd2 = sc.makeRDD(Seq("A","B","R","D","F","G","H","I","J","K","L","M","N"),4)

    // rdd2.zipWithIndex().foreach(println)

    rdd2.zipWithUniqueId().foreach(println)

  }

}
