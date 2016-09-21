package com.bigchange.datamining

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by C.J.YOU on 2016/9/13.
  */
object CustomNaiveBayes {

  type  CNB = (mutable.HashMap[(String,String),Int], mutable.HashMap[String,Int], mutable.HashMap[String,Int])

  // 训练贝叶斯模型
  def naiveBayeModel(lambda: Double, modelType:String, data:RDD[LabeledPoint] ): NaiveBayesModel = {

    val model = new NaiveBayes().setLambda(lambda).setModelType(modelType).run(data)

    model

  }
  // 不同数据训练的结果,id: 排除在外的Test数据集的id
  def model(id: Int,data: Map[Int,List[String]]): CNB = {

    // 统计非数值型数据处理方法
    val dHMap = new mutable.HashMap[(String,String),Int]()
    val hMap = new mutable.HashMap[String,Int]()
    val dMap = new mutable.HashMap[String,Int]()

    data.foreach { x =>
      val index = x._1
      if( index != id) {
        x._2.foreach(y => y.split(" ").toList.toArray match {
          case Array(f1,f2,f3,f4,la) =>

            if(hMap.contains(la)) {
              hMap.update(la,hMap(la) + 1)
            } else {
              hMap.put(la,1)
            }

           Array(f1,f2,f3,f4).foreach(x => {

             if(dHMap.contains((x,la))) dHMap.update((x,la),dHMap((x, la)) + 1)
             else {
              dHMap.put((x,la),1)
             }

             if(dMap.contains(x)) {
               dMap.update(x,dMap(x) + 1)
             } else {
               dMap.put(x,1)
             }
          })
        })
      }
    }

    (dHMap, hMap, dMap)

  }

  def predict(data:Array[String], modelNB: CNB) = {

    var posb = 1.0
    // 计算先验概率P(h)
    val listProb = new ListBuffer[(String, Double)]

    val totalH = modelNB._2.values.sum

    modelNB._2.foreach(h => {

      data.foreach(feature => {
        // D|h * h
        val value = modelNB._1.getOrElse((feature,h._1),0) /  ( h._2 * 1.0 )

        println("value:" + value + "," + feature + "," + h._1 + "," + h._2)

        posb *= value

      })

      posb *= ( modelNB._2(h._1) / ( totalH * 1.0 ) )

      listProb.+=((h._1,posb))

    })

    println("listProb:" + listProb)

    listProb.sortWith(_._2 > _._2).head
  }


  // 数值型数据 - 使用高斯分布的方式计算: 概率密度(pdf) D|h
  def pdf(mean:Double, ssd: Double, D:Double): Double = {

    val ePart = math.pow(math.E, -1 * math.pow(D - mean,2) / (2 * math.pow(ssd,2)))

    (1.0 / (math.sqrt(2 * math.Pi) * ssd)) * ePart
  }


  //  非结构化文本数据处理
  /*"""朴素贝叶斯分类器
        dataDir 训练集目录，子目录是分类，子目录中包含若干文本
        stopWordsPath 停词列表（一行一个）
  """
  */

  case class Prob(map:mutable.HashMap[String,AnyVal])
  case class Total(int: Int)

  def prepareData(dataDir: String, stopWordsPath: String): Unit = {
    // 获取stopword
    val stopwords = Source.fromFile(stopWordsPath).getLines().map(_.trim).toList
    // 将不是目录的元素过滤掉
    val dir = new File(dataDir)
   // 获取分类类别列表

  }

  // train: """计算分类下各单词出现的次数"""
  def trainData(dataDir: String, categories:List[String], stopWordsList:List[String]): Unit = {

    // 每个类别的统计数据保存在这两个map中
    val cateProb = new mutable.HashMap[String,Prob]()
    val cateCounts = new mutable.HashMap[String, Total]()

    var beforeRdd: RDD[(String,Int)] = null
    var finalVocalRdd:RDD[(String,Int)] = null
    // 对每个类别进行计算
    for(category <- categories) {

      val currentFiles = dataDir + "/" + category + "/*"
      val data = Source.fromFile(currentFiles).getLines().flatMap(_.split(" ")).map(_.toLowerCase).filter(x =>x != "" && !stopWordsList.contains(x)).map((_, 1)).toList
      val rdd = SparkContext.getOrCreate().parallelize(data).reduceByKey(_+_)
      finalVocalRdd = rdd.++(beforeRdd)
      beforeRdd = rdd
      val keyVale = rdd.collectAsMap().asInstanceOf[mutable.HashMap[String,AnyVal]]
      cateProb.+=((category,new Prob(keyVale)))
      cateCounts.+=((category, new Total(keyVale.values.asInstanceOf[Iterable[Int]].sum)))
    }
    // 所有数据的统计结果保存在这个map中
    val vocabulary = finalVocalRdd.collectAsMap().asInstanceOf[mutable.HashMap[String,Int]]
    // 删除出现次数小于3次的单词
    val finalVocal = vocabulary.filter(_._2 < 3)
    val lenVocal = finalVocal.size
    // 计算概率
    for(category <- categories) {

      val map = new mutable.HashMap[String,Double]

      val denominator = cateCounts(category).int + lenVocal

      finalVocal.foreach { x =>

        val count = cateProb.get(category).get.map.getOrElse(x._1, 1).asInstanceOf[Int]
        map.+=((category, (count + 1) / (denominator * 1.0)))

      }
      cateProb.update(category, new Prob(map.asInstanceOf[mutable.HashMap[String,AnyVal]]))

    }

  }

  // 用训练好的模型概率数据集分类
  def classify(file: String, vocal:mutable.HashMap[String,Int], categories:List[String], stopWordsList:List[String], modelProb:mutable.HashMap[String,Prob]): (String, Double) = {

    // 加载数据
    val test = Source.fromFile(file).getLines().flatMap(_.split(" ")).map(_.toLowerCase).filter(x =>x != "" && !stopWordsList.contains(x)).toList
    // 开始分类计算
    val resultList = new ListBuffer[(String, Double)]
    categories.foreach { y =>
      var result = 0.0
      test.foreach { x =>
        val prob = modelProb.get(y).get.map.get(x).get.asInstanceOf[Double]
        result += math.log(prob) // D1|h * D2|h2  采用取对数的形式：变为对数相加
      }
      resultList.+=((y, result))
    }

    resultList.sortBy(_._2).head

  }


}
