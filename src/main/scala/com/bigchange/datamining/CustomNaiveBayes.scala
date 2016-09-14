package com.bigchange.datamining

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
  // 不同数据训练的结果,id:Test数据集的id
  def model(id: Int,data: Map[Int,List[String]]): CNB = {

    val dHMap = new mutable.HashMap[(String,String),Int]()
    val hMap = new mutable.HashMap[String,Int]()
    val dMap = new mutable.HashMap[String,Int]()
    data.foreach { x =>
      val index = x._1
      if( index != id) {
        x._2.foreach(y => y.split(" ").toList.toArray match {
          case Array(f1,f2,f3,f4,la) =>
             Array(f1,f2,f3,f4).foreach(x => {

               if(dHMap.contains((x,la))) {
                dHMap.update((x,la),dHMap.get((x,la)).get + 1)
               } else {
                dHMap.put((x,la),1)
               }
               if(hMap.contains(x)) {
                 hMap.update(x,hMap.get(x).get + 1)
               } else {
                 hMap.put(x,1)
               }
               if(dMap.contains(x)) {
                 dMap.update(x,dMap.get(x).get + 1)
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
    val listProb = new ListBuffer[Double]
    modelNB._2.foreach(h => {
      posb = 1.0
      data.foreach(feature => {
        // D|h * h
        posb *= modelNB._1.getOrElse((feature,h._1),0)
      })
      posb *= modelNB._2.get(h._1).get
      listProb.+=(posb)
    })

    println("listProb:" + listProb)
  }

}
