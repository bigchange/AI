package com.bigchange.mllib

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by CHAOJIANG on 2016/9/25 0025.
  * 新闻数据中提取TF-IDF特征的实例
  */
object NewsGroups {

  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("Newsgroups").setMaster("local"))

  def main(args: Array[String]): Unit = {

    val path = ""
    val rdd = sc.wholeTextFiles(path)

    val tokens = tokenize(rdd)

    // 提取词干： 复杂（walking，walker -> walk）, 可以通过标准的NLP方法或者搜索引擎软件实现（NLTK, OpenNLP，Lucene）






  }

  // 分词组合函数和
  def tokenize(rdd: RDD[(String, String)]) = {

    // 分词方法
    val text = rdd.map { case(file, content) => content }
    val whiteSpaceSplite = text.flatMap(t => t.split(" ").map(_.toLowerCase))

    // 改进分词效果： 正则表达式切分原始文档， 由于许多不是单词字符
    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase))

    // 过滤到数字和包含数字的单词
    val regex = """[^0-9]*""".r
    val filterNumbers = nonWordSplit.filter(token => regex.pattern.matcher(token).matches)

    // 剔除停用词 : 有停用词列表可以参考
    val stopWordSList = List("the","a","an","of","or","in","for","by","on","but","is","not","with","as","was","if","they","this","are","and","it","have","from","at","my","be","that","to")

    val tokenCountsFilteredStopWords = filterNumbers.filter(!stopWordSList.contains(_)).map((_, 1)).filter{ case (k, v) => k.length >=2 }.reduceByKey(_ + _).filter{ case (k, v) => v >=2 }

    tokenCountsFilteredStopWords

  }

}
