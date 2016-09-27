package com.bigchange.test

import com.bigchange.datamining.CustomNaiveBayes

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by C.J.YOU on 2016/9/13.
  */
object NaiveBayesTest {

  /**
    * 数据 :
    * feature1: 健康（health）、外表（appearance）、两者皆是（both）
    * feature2: 很少运动（sedentary）、一般（moderate）、经常运动（active）
    * feature3: 热情是高（aggressive）还是一般（moderate）；
    * feature4: 最后，我们会问他是否适应使用高科技产品
    */
  def main(args: Array[String]) {

    // val Array(dataPath) = args
    val data = Source.fromFile("src/main/resources/nbData/i100-i500").getLines().toList
    // 可实现打完包后读取jar包中对应文件数据
    val data2 = Source.fromInputStream(this.getClass.getResourceAsStream("src/main/resources/nbData/i100-i500")).getLines().toList
    // 十折交叉验证(index,List(item1,item2))
    val splitData  = data.zipWithIndex.map(x => (x._2 % 10,x._1)).groupBy(_._1).mapValues(x => x.map(_._2))
    val modelMap = new mutable.HashMap[Int,String]()

    val model = CustomNaiveBayes.model(0, splitData)
    var list = List((0,model))

    for (id <- 1 until 10) {
      // 训练
      val model = CustomNaiveBayes.model(id, splitData)
      list = list ::: List((id,model))

    }

    // 分类
    val listP = new ListBuffer[(String, Double)]
    list.foreach(x => {
      println("model:" + x)
      val pred = CustomNaiveBayes.predict(Array("health", "moderate", "moderate1", "yes"), x._2)
     listP.+=(pred)
    })
    println("tobe:"+ listP)
    println("tobe:"+ listP.max)

  }

}
