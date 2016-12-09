package com.bigchange.test

/**
  * Created by C.J.YOU on 2016/9/27.
  * 处理数据过程中
  * 涉及到的一些转换的Test用例
  */

object Test {

  // val  sc = SparkContext.getOrCreate(new SparkConf().setMaster("local").setAppName("Test"))

  def main(args: Array[String]) {

    // @Test1 index唯一性测试
    /* val rdd2 = sc.makeRDD(Seq("A","B","R","D","F","G","H","I","J","K","L","M","N"),4)

     rdd2.zipWithIndex().foreach(println)

     rdd2.zipWithUniqueId().foreach(println)*/

    // @Test 中心化和零均值中心化
    /*val rdd = sc.makeRDD(Seq(Array(1.0,2.0,3.0),Array(4.0,5.0,6.0),Array(7.0,8.0,9.0)))

    val vectors = rdd.map(x => Vectors.dense(x))
    // Array[Double]是所库中类的通用变量形式
    val rowMatrix = new RowMatrix(vectors)

    /**
      * [1.0,2.0,3.0]
      * [4.0,5.0,6.0]
      * [7.0,8.0,9.0]
      */
    rowMatrix.rows.foreach(println)

    val means = rowMatrix.computeColumnSummaryStatistics().mean

    println("means:"+ means)  // means:[4.0,5.0,6.0]

    // 均值减法
    val bVectors = rowMatrix.rows.map(x => breeze.linalg.DenseVector(x.toArray) - breeze.linalg.DenseVector(means.toArray))
      .foreach(println)

    val scaler = new StandardScaler(withMean = true, withStd = false).fit(vectors) // 提取mean
    val scaledVectors = vectors.map { v => scaler.transform(v) } // 向量减去当前列的平均值 */

    // @ Test 权重小随机数初始化
   /* val random = new Random()
    val dimension = 10
    val weights = Array.tabulate(10)(_ => random.nextGaussian()) // 基于零均值和标准差的一个高斯分布（译者注：国内教程一般习惯称均值参数为期望\mu）来生成随机数的*/

  }

}
