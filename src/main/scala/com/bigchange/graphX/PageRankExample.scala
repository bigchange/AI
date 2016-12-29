package com.bigchange.graphx

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by C.J.YOU on 2016/4/20.
  * 一个有较多链入的页面会有较高的等级，相反如果一个页面没有任何链入页面，那么它没有等级。
  *  Google把从A页面到B页面的链接解释为A页面给B页面投票，一个页面不能投票2次，换句话说，根据链出总数平分一个页面的PR值
  *  如果给每个页面一个随机PageRank值（非0），那么经过不断的重复计算，这些页面的PR值会趋向于稳定，也就是收敛的状态
  *  在Sergey Brin和Lawrence Page的1998年原文中给每一个页面设定的最小值是1-d，而不是这里的：(1-d)/N
  */
object PageRankExample {

  // data format: # FromNodeId	ToNodeId

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <file> <iter>")
      System.exit(1)
    }

    showWarning()

    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local")
    val iters = if (args.length > 1) args(1).toInt else 10
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0), 10)

    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct()
      .groupByKey()
      .cache()
    // 每个页面一个随机PageRank值 : 1.0
    var ranks = links
      .mapValues(v => 1.0)  // 与 map 不同， mapValues 不改变分区的情况， 与后续的join 操作 会变得更加高效一点

    /**
      *  println("ranks:"+ranks.collect().foreach(println))
        (341117,1.0)
        (479683,1.0)
        (712764,1.0)
        (800487,1.0)
      */
    for (i <- 1 to iters) {
      val contribs = links
        .join(ranks)
        .values
        .flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size)) } // 每个url的贡献值平均

      // 每一个页面设定的最小值(0.15)
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)  // 可根据具体情况调整 公式

    }

    val output = ranks.sortBy(t => t._2, ascending = false).take(100) // 降序输出排名
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    ctx.stop()
  }
}
