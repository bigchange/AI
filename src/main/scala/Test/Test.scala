package test

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by C.J.YOU on 2016/3/14.
  */

object Test {
  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("USER")

  val sc = new SparkContext(sparkConf)
  def main(args: Array[String]) {
    sc.wholeTextFiles("hdfs://server:9000/data/kafka").foreach(println)
  }
}
