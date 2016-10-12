package com.bigchange.mllib

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by CHAOJIANG on 2016/9/25 0025.
  * 新闻数据中提取TF-IDF特征的实例
  * 贝叶斯多标签分类器
  */
object NewsGroups {

  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("Newsgroups").setMaster("local"))

  var swl: Broadcast[List[String]] = null
  var tokenFiltered: Broadcast[Set[String]] = null

  def main(args: Array[String]): Unit = {

    val path = "file:///F:/SmartData-X/DataSet/20news-bydate/20news-bydate-train/*/*"
    val rdd = sc.wholeTextFiles(path)

    // 分词方法
    val text = rdd.map { case(file, content) => content }
    // println("text:" + text.count)

    val whiteSpaceSplite = text.flatMap(t => t.split(" ").map(_.toLowerCase))

    // 改进分词效果： 正则表达式切分原始文档， 由于许多不是单词字符
    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase))
    // println("nonWordSplit:" + nonWordSplit.count)

    // 过滤到数字和包含数字的单词
    val regex = """[^0-9]*""".r
    val filterNumbers = nonWordSplit.filter(token => regex.pattern.matcher(token).matches)

    // println("all words:" + filterNumbers.count)

    // 剔除停用词 : 有停用词列表可以参考
    // val stopWordSList = List("the","a","an","of","or","in","for","by","on","but","is","not","with","as","was","if","they","this","are","and","it","have","from","at","my","be","that","to")

    val stopWordSList = Source.fromFile("src/main/resources/stopWords").getLines().toList

    val tokenCountsFiltered = filterNumbers.map((_, 1)).filter{ case (k, v) => k.length <2 }.reduceByKey(_ + _).filter{ case (k, v) => v < 2 }.map(_._1).collect.toSet

    val stopWordBro = sc.broadcast(stopWordSList)
    swl = stopWordBro
    val tokenCountsFilteredBro = sc.broadcast(tokenCountsFiltered)
    tokenFiltered = tokenCountsFilteredBro

    // 提取词干： 复杂（walking，walker -> walk）, 可以通过标准的NLP方法或者搜索引擎软件实现（NLTK, OpenNLP，Lucene）

    val tokens = text.map(doc => tokenizer(doc))

    // 使用TF-IDF处理文本
    // val tFIDF = TFIDFModel(rdd, tokens)

    val word2Vec = word2VecModel(tokens)


  }

  // Word2Vec 词项模型
  def word2VecModel(tokens:RDD[Seq[String]]) = {

    val word2Vec = new Word2Vec().setSeed(42) // 随机种子作为模型训练参数
    val word2VecModel = word2Vec.fit(tokens)
    // 获取相似单词
    word2VecModel.findSynonyms("hockey", 20).foreach(println)

    /*(ecac,1.3701449062603164)
    (rec,1.3661268683393772)
    (sport,1.3303370573154512)
    (hispanic,1.3007176914876915)
    (tournament,1.2907648102832736)
    (glens,1.2741494398054731)
    (ahl,1.232742342012108)
    (champs,1.2196663349162957)
    (octopi,1.2021684425511912)
    (sports,1.193158665614078)
    (motorcycles,1.1882885074872007)
    (woofers,1.1733295295882904)
    (expos,1.1717594763417933)
    (swedish,1.1709556679736088)
    (calder,1.1471181568937654)
    (affiliate,1.1425616781507797)
    (wabc,1.142089952672354)
    (woofing,1.1352360429053325)
    (ncaa,1.1334254449430632)
    (phils,1.1312980515841555)*/

  }

  // 每篇分词
  def tokenizer(line: String) = {

    line.split("""\W+""").map(_.toLowerCase)
      .filter(token => """[^0-9]*""".r.pattern.matcher(token).matches)
      .filterNot(token => swl.value.contains(token) || tokenFiltered.value.contains(token))
      .filter(token => token.length >= 2 )
      .toSeq

  }

  // TF-IDF 词项模型
  def TFIDFModel(rdd:RDD[(String, String)],tokens:RDD[Seq[String]]) = {

    // 处理成词项形式的文档以向量形式表达： HashingTF - 特征hash把输入的文本的词项映射为词频向量的下标
    // 维度参数

    println("tfidf fit started !!")
    val dimension = math.pow(2, 18).toInt
    val hashingTF = new HashingTF(dimension)

    val tf  = hashingTF.transform(tokens) // 每篇输入文档（词项的序列）映射到一个MLib的Vector对象
      .cache()


    val idf = new IDF().fit(tf) // 获取每个单词的逆向文本频率

    val tfidf = idf.transform(tf)
    println("tfidf fit over!!")

    val v2 = tfidf.first().asInstanceOf[SparseVector]

    // 观察整个文档TF-IDF最大和最小权重

    val minMaxVals = tfidf.map { v =>
      val sv = v.asInstanceOf[SparseVector]
      (sv.values.min, sv.values.max)
    }
    val globalMinMax = minMaxVals.reduce { case ((min1,max1),(min2, max2)) => (math.min(min1, min2), math.max(max1, max2)) }

    println("globalMinMax:" + globalMinMax)

    // 实例运用： 1. 计算文档的相似度（CosSim） 2.作为输入训练一个多标签的分类模型

    // 采用NB来处理多分类问题
    // 获取类别
    val newsGroups = rdd.map { case (file, doc) => file.split("/").takeRight(2).head }

    val newsGroupsMap = newsGroups.distinct().zipWithIndex.collectAsMap()

    println("newsGroupsMap: " + newsGroupsMap)

    // 两个RDD中元素的对应起来
    val zipped = newsGroups.zip(tfidf)

    val trainData = zipped.map { case (topic, vector) => LabeledPoint(newsGroupsMap(topic), vector)}
      .cache()

    val model = NaiveBayes.train(trainData, lambda =  0.1 )

    // model.save(sc, path = "file:///F:/SmartData-X/DataSet/20news-bydate/nbmodel")

    // TestData 预处理
    val testPath = "file:///F:/SmartData-X/DataSet/20news-bydate/20news-bydate-test/*/*"
    val testRdd = sc.wholeTextFiles(testPath)

    val testLabels = testRdd.map { case(file, content) => val topic = file.split("/").takeRight(2).head; newsGroupsMap(topic) }

    val  testTF = testRdd.map{ case (file, doc) => hashingTF.transform(tokenizer(doc)) }

    // Note: 这里使用训练集的IDF来转换测试集数据成TF-IDF向量
    val testTFIDF = idf.transform(testTF)

    val zippedTest = testLabels.zip(testTFIDF)

    val test = zippedTest.map { case (topic ,vector) => LabeledPoint(topic, vector) }

    // 预测
    val predictionAndLabels = test.map(p => (model.predict(p.features), p.label))

    val accuracy = 1.0 * predictionAndLabels.filter(x => x._1 == x._2).count / test.count

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println("accuracy:" + accuracy)
    println("加权F-指标：" + metrics.weightedFMeasure) // 加权F-指标：0.781142389463205

  }

}
