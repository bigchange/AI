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
  // 不同数据训练的结果,id: 排除在外的Test数据集的id
  def model(id: Int,data: Map[Int,List[String]]): CNB = {

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

}
